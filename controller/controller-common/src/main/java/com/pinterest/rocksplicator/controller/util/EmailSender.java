/*
 *  Copyright 2017 Pinterest, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.pinterest.rocksplicator.controller.util;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

/**
 * Send email from localhost.
 *
 */
public class EmailSender {
  public static final Logger LOG = LoggerFactory.getLogger(EmailSender.class);

  private Properties properties;
  private String fromAddress;
  private String toAddress;
  private Session session;

  public EmailSender(String fromAddress, String toAddress) {
    this.fromAddress = fromAddress;
    this.toAddress = toAddress;
    this.properties = System.getProperties();
    this.properties.setProperty("mail.smtp.host", "localhost");
    this.session = Session.getDefaultInstance(properties);
  }

  public boolean sendEmail(String title, String content) {
    // No-op if fromAddress / toAddress is empty
    if (!Strings.isNullOrEmpty(fromAddress) && !Strings.isNullOrEmpty(toAddress)) {
      try {
        MimeMessage message = new MimeMessage(session);
        message.setFrom(new InternetAddress(fromAddress));
        message.addRecipient(Message.RecipientType.TO, new InternetAddress(toAddress));
        message.setSubject(title);
        message.setText(content);
        Transport.send(message);
      } catch (Exception e) {
        LOG.error("Cannot send email: ", e.getCause());
        return false;
      }
    }
    return true;
  }
}
