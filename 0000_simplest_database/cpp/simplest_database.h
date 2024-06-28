#pragma once
#include <string>

class SimplestDatabase {
private:
  std::string filename;

public:
  SimplestDatabase(const std::string &filename = "database");

  void set(const std::string &key, const std::string &value);

  std::string get(const std::string &key);
};
