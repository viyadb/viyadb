#ifndef VIYA_CLUSTER_CONFIGURATOR_H_
#define VIYA_CLUSTER_CONFIGURATOR_H_

namespace viya { namespace util { class Config; }}
namespace viya { namespace cluster { class Controller; }}

namespace viya {
namespace cluster {

/**
 * Configures workers on this node
 */
class Configurator {
  public:
    Configurator(const Controller& controller, const std::string& load_prefix_);
    Configurator(const Configurator& other) = delete;

    void ConfigureWorkers();

  protected:
    void AdaptTableConfig(util::Config& table_config);
    void CreateTables(const util::Config& worker_config);

  private:
    const Controller& controller_;
    const std::string load_prefix_;
};

}}

#endif // VIYA_CLUSTER_CONFIGURATOR_H_
