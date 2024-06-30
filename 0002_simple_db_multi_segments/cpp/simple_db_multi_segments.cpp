#include "simple_db_multi_segments.h"
#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>

const char *IsADirectoryError::what() const noexcept {
  return "Not a directory, or the directory isn't formatted correctly";
}

Index::Index(const std::string &segment_name)
    : segment_name(segment_name), cursor(0) {}

void Index::add_next(const std::string &line, const std::string &key) {
  std::string actual_key = key;
  if (key.empty()) {
    size_t pos = line.find(',');
    actual_key = line.substr(0, pos);
  }
  size_t length = line.length();
  idx_map[actual_key] = {cursor, length};
  cursor += length;
}

std::pair<size_t, size_t> Index::get(const std::string &key) const {
  auto it = idx_map.find(key);
  if (it != idx_map.end()) {
    return it->second;
  }
  throw std::runtime_error("Key not found");
}

const std::string &Index::get_segment_name() const { return segment_name; }

SimpleDbMultiSegments::SimpleDbMultiSegments(const std::string &dbname,
                                             size_t segment_bytes_threshold)
    : dbname(dbname), indexes_loaded(false),
      segment_bytes_threshold(std::max(segment_bytes_threshold, size_t(1))) {
  check_db_directory();
  load_indexes();
}

void SimpleDbMultiSegments::set(const std::string &key,
                                const nlohmann::json &json_dict) {
  if (indexes.empty() ||
      indexes.back()->get_cursor() >= segment_bytes_threshold) {
    auto index = std::make_shared<Index>(
        dbname + "/segment_" +
        std::to_string(get_epoch_time_in_microseconds()) + ".db");
    indexes.push_back(index);
  }
  auto index = indexes.back();
  std::string line = key + "," + json_dict.dump() + "\n";
  index->add_next(line, key);
  std::ofstream file(index->get_segment_name(), std::ios::app);
  file << line;
}

nlohmann::json SimpleDbMultiSegments::get(const std::string &key) {
  for (auto it = indexes.rbegin(); it != indexes.rend(); ++it) {
    try {
      auto [offset, length] = (*it)->get(key);
      std::ifstream file((*it)->get_segment_name());
      file.seekg(offset);
      std::string line(length, '\0');
      file.read(&line[0], length);
      return nlohmann::json::parse(line.substr(line.find(',') + 1));
    } catch (const std::exception &) {
      continue;
    }
  }
  return nullptr;
}

void SimpleDbMultiSegments::compact(size_t new_segment_bytes_threshold) {
  size_t current_segment_bytes_threshold = new_segment_bytes_threshold
                                               ? new_segment_bytes_threshold
                                               : segment_bytes_threshold;

  std::set<std::string> checked_keys;
  std::vector<std::shared_ptr<Index>> new_indexes;
  auto new_index = std::make_shared<Index>(
      dbname + "/segment_" + std::to_string(get_epoch_time_in_microseconds()) +
      ".db");

  for (auto it = indexes.rbegin(); it != indexes.rend(); ++it) {
    for (const auto &[key, _] : (*it)->get_idx_map()) {
      if (checked_keys.find(key) != checked_keys.end())
        continue;
      checked_keys.insert(key);

      if (new_index->get_cursor() >= current_segment_bytes_threshold) {
        new_indexes.push_back(new_index);
        new_index = std::make_shared<Index>(
            dbname + "/segment_" +
            std::to_string(get_epoch_time_in_microseconds()) + ".db");
      }

      nlohmann::json value = get(key);
      std::string line = key + "," + value.dump() + "\n";
      new_index->add_next(line, key);
      std::ofstream file(new_index->get_segment_name(), std::ios::app);
      file << line;
    }
  }

  if (!new_index->get_idx_map().empty()) {
    new_indexes.push_back(new_index);
  }

  indexes = std::move(new_indexes);
}

void SimpleDbMultiSegments::check_db_directory() {
  std::filesystem::path directory(dbname);
  std::filesystem::path marker = directory / ".simple_db_multi_segments_marker";

  if (std::filesystem::exists(directory)) {
    if (std::filesystem::is_directory(directory)) {
      if (!std::filesystem::exists(marker)) {
        throw IsADirectoryError();
      }
    } else {
      throw IsADirectoryError();
    }
  } else {
    std::filesystem::create_directory(directory);
    std::ofstream(marker).close();
  }
}

void SimpleDbMultiSegments::load_index(Index &index) {
  std::ifstream file(index.get_segment_name());
  std::string line;
  while (std::getline(file, line)) {
    index.add_next(line + "\n");
  }
}

void SimpleDbMultiSegments::load_indexes() {
  if (!indexes_loaded) {
    auto directory = std::filesystem::path(dbname);
    for (const auto &entry : std::filesystem::directory_iterator(directory)) {
      if (entry.path().filename().string().find("segment_") == 0) {
        auto index = std::make_shared<Index>(entry.path().string());
        indexes.push_back(index);
        load_index(*index);
      }
    }
    indexes_loaded = true;
  }
}

int64_t SimpleDbMultiSegments::get_epoch_time_in_microseconds() const {
  auto now = std::chrono::system_clock::now();
  auto epoch = now.time_since_epoch();
  auto microseconds =
      std::chrono::duration_cast<std::chrono::microseconds>(epoch).count();
  return microseconds;
}