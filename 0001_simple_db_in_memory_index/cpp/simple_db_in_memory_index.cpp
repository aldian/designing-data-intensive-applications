#include "simple_db_in_memory_index.h"
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <utility>

_Index::_Index() : _cursor(0) {}

void _Index::add_next(const std::string &line, const std::string &key) {
  std::string actual_key = key;
  if (key.empty()) {
    std::istringstream iss(line);
    getline(iss, actual_key, ',');
  }
  size_t length = line.length();
  _idx_map[actual_key] = {_cursor, length};
  _cursor += length;
}

std::pair<long, size_t> _Index::get(const std::string &key) const {
  auto it = _idx_map.find(key);
  if (it != _idx_map.end()) {
    return it->second;
  }
  throw std::runtime_error("Key not found");
}

SimpleDbInMemoryIndex::SimpleDbInMemoryIndex(const std::string &filename)
    : filename(filename), _index() {
  std::ifstream file(filename);
  if (!file.is_open()) {
    return;
  }

  std::string line;
  while (getline(file, line)) {
    _index.add_next(line + "\n");
  }
}

void SimpleDbInMemoryIndex::set(const std::string &key,
                                const nlohmann::json &json_dict) {
  std::string line = key + "," + json_dict.dump() + "\n";
  std::ofstream file(filename, std::ios::app);
  file << line;
  file.close();
  _index.add_next(line, key);
}

nlohmann::json SimpleDbInMemoryIndex::get(const std::string &key) {
  std::pair<long, size_t> offset_length;
  try {
    offset_length = _index.get(key);
  } catch (const std::runtime_error &) {
    return nullptr;
  }

  std::ifstream file(filename);
  file.seekg(offset_length.first);
  std::string line;
  line.resize(offset_length.second);
  file.read(&line[0], offset_length.second);
  file.close();
  return nlohmann::json::parse(line.substr(line.find(',') + 1));
}