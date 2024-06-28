#include "simplest_database.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

SimplestDatabase::SimplestDatabase(const std::string &filename)
    : filename(filename) {}

void SimplestDatabase::set(const std::string &key,
                           const nlohmann::json &json_dict) {
  std::ofstream file;
  file.open(filename, std::ios::app);
  if (!file.is_open()) {
    throw std::runtime_error("Unable to open file for writing");
  }
  file << key << "," << json_dict.dump() << "\n";
  file.close();
}

nlohmann::json SimplestDatabase::get(const std::string &key) {
  std::ifstream file(filename);
  if (!file.is_open()) {
    throw std::runtime_error("Unable to open file for reading");
  }

  std::vector<std::string> matching_rows;
  std::string line;
  while (getline(file, line)) {
    std::istringstream iss(line);
    std::string k, v;
    if (getline(iss, k, ',') && getline(iss, v)) {
      if (k == key) {
        matching_rows.push_back(v);
      }
    }
  }
  file.close();

  if (matching_rows.empty()) {
    return nullptr;
  } else {
    return nlohmann::json::parse(matching_rows.back());
  }
}