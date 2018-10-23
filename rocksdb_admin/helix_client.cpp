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

namespace {

JNIEnv* createVM(const std::string& class_path) {
  JavaVM* jvm;
  JNIEnv* env;
  JavaVMInitArgs args;
  JavaVMOption options[3];

  args.version = JNI_VERSION_1_6;
  args.nOptions = 3;
  const std::string env_class_path = std::getenv("CLASSPATH");
  auto class_opt = "-Djava.class.path=" + class_path +
      "cluster_management-0.0.1-SNAPSHOT-jar-with-dependencies.jar" +
      env_class_path;
  LOG(INFO) << "classpath=" << class_opt;
  options[0].optionString = const_cast<char*>(class_opt.c_str());
  options[1].optionString = "-verbose:jni";
  // use -Xrs to block JVM signal handling
  options[2].optionString = "-Xrs";

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
                 const std::string& config_post_url) {
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

  args = env->NewObjectArray(14, env->FindClass("java/lang/String"), nullptr);

  env->SetObjectArrayElement(args, 0, env->NewStringUTF("--zkSvr"));
  env->SetObjectArrayElement(
    args, 1, env->NewStringUTF(zk_connect_str.c_str()));
  env->SetObjectArrayElement(args, 2, env->NewStringUTF("--cluster"));
  env->SetObjectArrayElement(args, 3, env->NewStringUTF(cluster.c_str()));
  env->SetObjectArrayElement(args, 4, env->NewStringUTF("--host"));
  auto ip = common::getLocalIPAddress();
  env->SetObjectArrayElement(args, 5, env->NewStringUTF(ip.c_str()));
  env->SetObjectArrayElement(args, 6, env->NewStringUTF("--port"));
  auto port = std::to_string(FLAGS_port);
  env->SetObjectArrayElement(args, 7, env->NewStringUTF(port.c_str()));
  env->SetObjectArrayElement(args, 8, env->NewStringUTF("--stateModelType"));
  env->SetObjectArrayElement(
    args, 9, env->NewStringUTF(state_model_type.c_str()));
  env->SetObjectArrayElement(args, 10, env->NewStringUTF("--domain"));
  env->SetObjectArrayElement(args, 11, env->NewStringUTF(domain.c_str()));
  env->SetObjectArrayElement(args, 12, env->NewStringUTF("--configPostUrl"));
  env->SetObjectArrayElement(args, 13,
                             env->NewStringUTF(config_post_url.c_str()));

  env->CallStaticVoidMethod(ParticipantClass, mainMethod, args);

  // The participant main function should never exit
  env->ExceptionDescribe();
  CHECK(false) << "Participant main() exited";
}

}

namespace admin {

void JoinCluster(const std::string& zk_connect_str,
                 const std::string& cluster,
                 const std::string& state_model_type,
                 const std::string& domain,
                 const std::string& class_path,
                 const std::string& config_post_url) {
  std::thread t([zk_connect_str, cluster, state_model_type, domain,
                 class_path, config_post_url] () {
      // FIXME use a more reliable way to ensure the thread calling JoinCluster
      // has time to start the thrift server.
      std::this_thread::sleep_for(std::chrono::seconds(10));

      auto env = createVM(class_path);
      invokeClass(env, zk_connect_str, cluster, state_model_type, domain,
                  config_post_url);
    });

  t.detach();

  LOG(INFO) << "Launching JVM and starting Helix participant";
}

}  // namespace admin
