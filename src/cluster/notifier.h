/*
 * Copyright (c) 2017-present ViyaDB Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef VIYA_CLUSTER_NOTIFIER_H_
#define VIYA_CLUSTER_NOTIFIER_H_

#include <functional>
#include <memory>
#include <vector>

namespace viya {
namespace util {
class Config;
}
}

namespace viya {
namespace cluster {

class Message;

enum IndexerType { REALTIME, BATCH };

class MessageProcessor {
public:
  virtual ~MessageProcessor() = default;
  virtual bool ProcessMessage(const std::string &indexer_id,
                              const Message &message) = 0;
};

class Notifier {
public:
  Notifier(const std::string &indexer_id) : indexer_id_(indexer_id) {}
  virtual ~Notifier() = default;

  const std::string indexer_id() const { return indexer_id_; }

  virtual void Listen(MessageProcessor &processor) = 0;
  virtual std::vector<std::unique_ptr<Message>> GetAllMessages() = 0;
  virtual std::unique_ptr<Message> GetLastMessage() = 0;

protected:
  const std::string indexer_id_;
};

class NotifierFactory {
public:
  static Notifier *Create(const std::string &indexer_id,
                          const util::Config &notifier_conf,
                          IndexerType indexer_type);
};

class MessageFactory {
public:
  static std::unique_ptr<Message> Create(const std::string &message,
                                         IndexerType indexer_type);
};
}
}

#endif // VIYA_CLUSTER_NOTIFIER_H_
