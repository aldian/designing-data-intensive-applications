#pragma once

#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>

class _Index {
public:
  _Index();
  void add_next(const std::string &line, const std::string &key = "");
  std::pair<long, size_t> get(const std::string &key) const;

  const std::unordered_map<std::string, std::pair<long, size_t>> &
  get_idx_map() const {
    return _idx_map;
  }

private:
  std::unordered_map<std::string, std::pair<long, size_t>> _idx_map;
  long _cursor;
};

class SimpleDbInMemoryIndex {
public:
  SimpleDbInMemoryIndex(const std::string &filename = "database");
  void set(const std::string &key, const nlohmann::json &json_dict);
  nlohmann::json get(const std::string &key);

  const _Index &get_index() const { return _index; }
  const std::string &get_filename() const { return filename; }

private:
  std::string filename;
  _Index _index;
};