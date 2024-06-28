#pragma once
#include <nlohmann/json.hpp>
#include <string>

class SimplestDatabase {
private:
  std::string filename;

public:
  SimplestDatabase(const std::string &filename = "database");

  void set(const std::string &key, const nlohmann::json &json_dict);

  nlohmann::json get(const std::string &key);
};