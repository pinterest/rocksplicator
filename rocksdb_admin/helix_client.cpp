/// Copyright 2018 Pinterest Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0

/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

//
// @author bol (bol@pinterest.com)
//

#include "rocksdb_admin/helix_client.h"

#include "jni.h"

#include <chrono>
#include <thread>
#include <glog/logging.h>

#include "common/network_util.h"

DECLARE_int32(port);
DEFINE_string(helix_log_config_file_path, "",
    "Participant log config file path "
    "e.g: /etc/config/helix/log4j.properties");
DEFINE_bool(use_s3_backup, false,
            "whether we should use S3 env for rocksdb backup&rockstore");
DEFINE_string(s3_bucket_backup, "pinterest-jackson",
              "S3 bucket would be used for rocksdb backup & restore");

DEFINE_string(handoff_event_history_zkSvr,
  "",
  "[Optional]: zk server to store event history for leader handoff events");
DEFINE_string(handoff_event_history_config_path,
  "",
  "[Optional]: config path providing resources for which we need to log event history for leader events");
DEFINE_string(handoff_event_history_config_type,
  "",
  "[Optional]: Must be one of JSON_ARRAY / LINE_TERMINATED");

DEFINE_string(handoff_client_event_history_json_shard_map_path,
  "",
  "[Optional]: path of shard_map generated from spectator in json format");

namespace {

JNIEnv* createVM(const std::string& class_path) {
  JavaVM* jvm;
  JNIEnv* env;
  JavaVMInitArgs args;
  JavaVMOption options[10];

  args.version = JNI_VERSION_1_6;
  args.nOptions = 10;
  const std::string env_class_path = std::getenv("CLASSPATH");
  auto class_opt = "-Djava.class.path=" + class_path +
      "cluster_management-0.0.1-SNAPSHOT-jar-with-dependencies.jar" +
      env_class_path;
  auto helix_log_config_file_path = "-Dlog4j.configuration=file:" + FLAGS_helix_log_config_file_path;
  LOG(INFO) << "classpath=" << class_opt;
  options[0].optionString = const_cast<char*>(class_opt.c_str());
  options[1].optionString = "-verbose:jni";
  // use -Xrs to block JVM signal handling
  options[2].optionString = "-Xrs";
  options[3].optionString = const_cast<char*>(helix_log_config_file_path.c_str());
  options[4].optionString = "-Dcom.sun.management.jmxremote";
  options[5].optionString = "-Dcom.sun.management.jmxremote.port=10102";
  options[6].optionString = "-Dcom.sun.management.jmxremote.local.only=false";
  options[7].optionString = "-Dcom.sun.management.jmxremote.authenticate=false";
  options[8].optionString = "-Dcom.sun.management.jmxremote.ssl=false";
  options[9].optionString = "-Dcom.sun.management.jmxremote.rmi.port=10103";

  args.options = options;
  args.ignoreUnrecognized = false;

  auto ret = JNI_CreateJavaVM(&jvm, (void **)&env, &args);
  CHECK(ret == JNI_OK) << "Failed to create JVM: " << ret;
  return env;
}

void invokeClass(JNIEnv* env,
                 const std::string& zk_connect_str,
                 const std::string& cluster,
                 const std::string& state_model_type,
                 const std::string& domain,
                 const std::string& config_post_url,
                 const bool disable_spectator) {
  jclass ParticipantClass;
  jmethodID mainMethod;
  jobjectArray args;

  ParticipantClass = env->FindClass("com/pinterest/rocksplicator/Participant");
  if (!ParticipantClass) {
    env->ExceptionDescribe();
    CHECK(false);
  }

  mainMethod = env->GetStaticMethodID(ParticipantClass,
                                      "main",
                                      "([Ljava/lang/String;)V");

  std::vector<std::string> arguments;

  arguments.push_back(std::string("--zkSvr"));
  arguments.push_back(std::string(zk_connect_str));
  arguments.push_back(std::string("--cluster"));
  arguments.push_back(std::string(cluster));
  arguments.push_back(std::string("--host"));
  arguments.push_back(std::string(common::getLocalIPAddress()));
  arguments.push_back(std::string("--port"));
  arguments.push_back(std::string(std::to_string(FLAGS_port)));
  arguments.push_back(std::string("--stateModelType"));
  arguments.push_back(std::string(state_model_type));
  arguments.push_back(std::string("--domain"));
  arguments.push_back(std::string(domain));
  arguments.push_back(std::string("--configPostUrl"));
  arguments.push_back(std::string(config_post_url));
  if (FLAGS_use_s3_backup) {
    arguments.push_back(std::string("--s3Bucket"));
    arguments.push_back(std::string(FLAGS_s3_bucket_backup));
  }
  if (disable_spectator) {
    arguments.push_back(std::string("--disableSpectator"));
  }
  if (!FLAGS_handoff_event_history_zkSvr.empty()) {
    arguments.push_back(std::string("--handoffEventHistoryzkSvr"));
    arguments.push_back(std::string(FLAGS_handoff_event_history_zkSvr));
  }
  if (!FLAGS_handoff_event_history_config_path.empty()) {
    arguments.push_back(std::string("--handoffEventHistoryConfigPath"));
    arguments.push_back(std::string(FLAGS_handoff_event_history_config_path));
  }
  if (!FLAGS_handoff_event_history_config_type.empty()) {
    arguments.push_back(std::string("--handoffEventHistoryConfigType"));
    arguments.push_back(std::string(FLAGS_handoff_event_history_config_type));
  }
  if (!FLAGS_handoff_client_event_history_json_shard_map_path.empty()) {
    arguments.push_back(std::string("--handoffClientEventHistoryJsonShardMapPath"));
    arguments.push_back(std::string(FLAGS_handoff_client_event_history_json_shard_map_path));
  }


  int totalArgumentsSize = arguments.size();


  args = env->NewObjectArray(totalArgumentsSize, env->FindClass("java/lang/String"), nullptr);

  int argumentNum = 0;
  for (const std::string& nextArg : arguments) {
    env->SetObjectArrayElement(args, argumentNum, env->NewStringUTF(nextArg.c_str()));
    ++argumentNum;
  }

  env->CallStaticVoidMethod(ParticipantClass, mainMethod, args);

  // The participant main function should never exit
  env->ExceptionDescribe();
  CHECK(false) << "Participant main() exited";
}

} // namespace

namespace admin {

void JoinCluster(const std::string& zk_connect_str,
                 const std::string& cluster,
                 const std::string& state_model_type,
                 const std::string& domain,
                 const std::string& class_path,
                 const std::string& config_post_url,
                 const bool disable_spectator) {
  std::thread t([zk_connect_str, cluster, state_model_type, domain,
                 class_path, config_post_url, disable_spectator] () {
      // FIXME use a more reliable way to ensure the thread calling JoinCluster
      // has time to start the thrift server.
      std::this_thread::sleep_for(std::chrono::seconds(10));

      auto env = createVM(class_path);
      invokeClass(env, zk_connect_str, cluster, state_model_type, domain,
                  config_post_url, disable_spectator);
    });

  t.detach();

  LOG(INFO) << "Launching JVM and starting Helix participant";
}

// Destroy the JVM, which will call the shutdown handler registers
void DisconnectHelixManager() {
    JNIEnv* env;
    JavaVM* jvm;
    jclass ParticipantClass;
    jmethodID shutDownParticipantMethod;
    jsize nVMs;

    // Get the a reference to the created javaVM
    JNI_GetCreatedJavaVMs(NULL, 0, &nVMs);
    JavaVM** buffer = new JavaVM*[nVMs];
    JNI_GetCreatedJavaVMs(buffer, nVMs, &nVMs);

    if (nVMs != 1) {
        LOG(ERROR) << "There are " << nVMs << " created, expected 1";
        return;
    }
    jvm = buffer[0];

    jint result = jvm->GetEnv((void **) &env, JNI_VERSION_1_6);
    if (result == JNI_EDETACHED) {
        LOG(ERROR) << "EDATCHED, trying to attach thread";
        result = jvm->AttachCurrentThread((void**)&env, NULL);
    }

    if (result != JNI_OK) {
        LOG(ERROR) << "Failed to get env";
        return;
    }

    ParticipantClass = env->FindClass("com/pinterest/rocksplicator/Participant");
    if (!ParticipantClass) {
        env->ExceptionDescribe();
        LOG(ERROR) << "Failed to find Participant class";
        return;
    }

    shutDownParticipantMethod = env->GetStaticMethodID(ParticipantClass,
                                            "shutDownParticipant",
                                            "()V");
    if (!shutDownParticipantMethod) {
        LOG(ERROR) << "Failed to GetStaticMethodID";
        env->ExceptionDescribe();
        return;
    }

    LOG(INFO) << "Disconnecting helix manager";
    env->CallStaticVoidMethod(ParticipantClass, shutDownParticipantMethod);
}

}  // namespace admin
