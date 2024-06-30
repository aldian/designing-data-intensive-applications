#include "simple_db_multi_segments.h"
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <iostream>
#include <nlohmann/json.hpp>

namespace fs = std::filesystem;

// Utility function to remove directory if exists
void remove_directory(const std::string &dir) {
  if (fs::exists(dir)) {
    fs::remove_all(dir);
  }
}

class SimpleDbMultiSegmentsTest : public ::testing::Test {
protected:
  std::string dbname = "testdb";
  SimpleDbMultiSegments *db;

  void SetUp() override {
    remove_directory(dbname);
    db = new SimpleDbMultiSegments(dbname, 50); // segment_bytes_threshold = 50
    db->set("greeting", {{"hello", "world"}});
    db->set("micu", {{"species", "cat"}, {"color", "black"}, {"age", 3}});
    db->set("menu", {{"breakfast", "bubur ayam"},
                     {"lunch", "nasi rendang"},
                     {"dinner", "nasi goreng"}});
  }

  void TearDown() override {
    delete db;
    remove_directory(dbname);
  }
};

TEST_F(SimpleDbMultiSegmentsTest, ExistingDb) {
  SimpleDbMultiSegments db2(dbname);

  auto menu = db2.get("menu");
  auto greeting = db2.get("greeting");
  auto micu = db2.get("micu");

  EXPECT_EQ(menu, nlohmann::json({{"breakfast", "bubur ayam"},
                                  {"lunch", "nasi rendang"},
                                  {"dinner", "nasi goreng"}}));
  EXPECT_EQ(greeting, nlohmann::json({{"hello", "world"}}));
  EXPECT_EQ(micu, nlohmann::json(
                      {{"species", "cat"}, {"color", "black"}, {"age", 3}}));

  EXPECT_EQ(db2.get_indexes().size(), 2);
  EXPECT_NE(db2.get_indexes()[0]->get_segment_name(),
            db2.get_indexes()[1]->get_segment_name());
  EXPECT_TRUE(db2.get_indexes()[0]->get_idx_map().find("greeting") !=
              db2.get_indexes()[0]->get_idx_map().end());
  EXPECT_TRUE(db2.get_indexes()[0]->get_idx_map().find("micu") !=
              db2.get_indexes()[0]->get_idx_map().end());
  EXPECT_TRUE(db2.get_indexes()[1]->get_idx_map().find("menu") !=
              db2.get_indexes()[1]->get_idx_map().end());
}

TEST_F(SimpleDbMultiSegmentsTest, Set) {
  std::ifstream file1(db->get_indexes()[0]->get_segment_name());
  std::ifstream file2(db->get_indexes()[1]->get_segment_name());

  std::string line;
  std::getline(file1, line);
  EXPECT_EQ(line, "greeting,{\"hello\":\"world\"}");
  std::getline(file1, line);
  EXPECT_EQ(line, "micu,{\"age\":3,\"color\":\"black\",\"species\":\"cat\"}");

  std::getline(file2, line);
  EXPECT_EQ(line, "menu,{\"breakfast\":\"bubur ayam\",\"dinner\":\"nasi "
                  "goreng\",\"lunch\":\"nasi rendang\"}");
}

TEST_F(SimpleDbMultiSegmentsTest, Get) {
  auto menu = db->get("menu");
  auto greeting = db->get("greeting");
  auto micu = db->get("micu");

  EXPECT_EQ(menu, nlohmann::json({{"breakfast", "bubur ayam"},
                                  {"lunch", "nasi rendang"},
                                  {"dinner", "nasi goreng"}}));
  EXPECT_EQ(greeting, nlohmann::json({{"hello", "world"}}));
  EXPECT_EQ(micu, nlohmann::json(
                      {{"species", "cat"}, {"color", "black"}, {"age", 3}}));
}

TEST_F(SimpleDbMultiSegmentsTest, GetInvalidKey) {
  auto result = db->get("invalid key");
  EXPECT_EQ(result, nullptr);
}

TEST_F(SimpleDbMultiSegmentsTest, InvalidDbFolder) {
  fs::remove(dbname + "/.simple_db_multi_segments_marker");
  EXPECT_THROW(SimpleDbMultiSegments db2(dbname), IsADirectoryError);
}

TEST_F(SimpleDbMultiSegmentsTest, NameConflict) {
  remove_directory(dbname);
  std::ofstream(dbname).close();
  EXPECT_THROW(SimpleDbMultiSegments db2(dbname), IsADirectoryError);
}

TEST_F(SimpleDbMultiSegmentsTest, Compact) {
  db->set("greeting", {{"halo", "dunia"}});

  EXPECT_EQ(db->get_indexes().size(), 3);
  std::set<std::string> before_keys;
  for (const auto &index : db->get_indexes()) {
    for (const auto &pair : index->get_idx_map()) {
      before_keys.insert(pair.first);
    }
  }

  EXPECT_EQ(before_keys.size(), 3);

  std::map<std::string, nlohmann::json> values_before_compact;
  for (const auto &key : before_keys) {
    values_before_compact[key] = db->get(key);
  }

  db->compact();

  EXPECT_EQ(db->get_indexes().size(), 2);
  std::set<std::string> after_keys;
  for (const auto &index : db->get_indexes()) {
    for (const auto &pair : index->get_idx_map()) {
      after_keys.insert(pair.first);
    }
  }

  EXPECT_EQ(before_keys, after_keys);

  std::map<std::string, nlohmann::json> values_after_compact;
  for (const auto &key : before_keys) {
    values_after_compact[key] = db->get(key);
  }

  EXPECT_EQ(values_before_compact, values_after_compact);
}

TEST_F(SimpleDbMultiSegmentsTest, CompactWithNewBytesThreshold) {
  db->set("greeting", {{"halo", "dunia"}});

  EXPECT_EQ(db->get_indexes().size(), 3);
  std::set<std::string> before_keys;
  for (const auto &index : db->get_indexes()) {
    for (const auto &pair : index->get_idx_map()) {
      before_keys.insert(pair.first);
    }
  }

  EXPECT_EQ(before_keys.size(), 3);

  std::map<std::string, nlohmann::json> values_before_compact;
  for (const auto &key : before_keys) {
    values_before_compact[key] = db->get(key);
  }

  db->compact(1000000);

  EXPECT_EQ(db->get_indexes().size(), 1);
  std::set<std::string> after_keys;
  for (const auto &index : db->get_indexes()) {
    for (const auto &pair : index->get_idx_map()) {
      after_keys.insert(pair.first);
    }
  }

  EXPECT_EQ(before_keys, after_keys);

  std::map<std::string, nlohmann::json> values_after_compact;
  for (const auto &key : before_keys) {
    values_after_compact[key] = db->get(key);
  }

  EXPECT_EQ(values_before_compact, values_after_compact);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}