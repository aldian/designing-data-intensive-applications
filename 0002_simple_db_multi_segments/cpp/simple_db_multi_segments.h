#pragma once

#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <vector>

class IsADirectoryError : public std::exception {
public:
  const char *what() const noexcept override;
};

class Index {
public:
  Index(const std::string &segment_name);
  void add_next(const std::string &line, const std::string &key = "");
  std::pair<size_t, size_t> get(const std::string &key) const;
  const std::string &get_segment_name() const;

  size_t get_cursor() const { return cursor; }
  const std::unordered_map<std::string, std::pair<size_t, size_t>> &
  get_idx_map() const {
    return idx_map;
  }

private:
  std::string segment_name;
  std::unordered_map<std::string, std::pair<size_t, size_t>> idx_map;
  size_t cursor;
};

class SimpleDbMultiSegments {
public:
  SimpleDbMultiSegments(const std::string &dbname = "database",
                        size_t segment_bytes_threshold = 1024 * 1024);
  void set(const std::string &key, const nlohmann::json &json_dict);
  nlohmann::json get(const std::string &key);
  void compact(size_t new_segment_bytes_threshold = 0);

  const std::vector<std::shared_ptr<Index>> &get_indexes() const {
    return indexes;
  }

private:
  std::string dbname;
  std::vector<std::shared_ptr<Index>> indexes;
  bool indexes_loaded;
  size_t segment_bytes_threshold;

  void check_db_directory();
  void load_index(Index &index);
  void load_indexes();
  int64_t get_epoch_time_in_microseconds() const;
};