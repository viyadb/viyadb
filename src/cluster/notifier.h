/*
 * Copyright (c) 2017 ViyaDB Group
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
#include <vector>
#include <memory>

namespace viya { namespace util { class Config; }}

namespace viya {
namespace cluster {

class Info;

enum IndexerType { REALTIME, BATCH };

class Notifier {
  public:
    virtual ~Notifier() = default;
    virtual void Listen(std::function<void(const Info& info)> callback) = 0;
    virtual std::vector<std::unique_ptr<Info>> GetAllMessages() = 0;
    virtual std::unique_ptr<Info> GetLastMessage() = 0;
};

class NotifierFactory {
  public:
    static Notifier* Create(const util::Config& notifier_conf, IndexerType indexer_type);
};

class InfoFactory {
  public:
    static std::unique_ptr<Info> Create(const std::string& info, IndexerType indexer_type);
};

}}

#endif // VIYA_CLUSTER_NOTIFIER_H_
