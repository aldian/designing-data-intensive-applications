#include "simple_db_in_memory_index.h"
#include <cstdio>
#include <fstream>
#include <gtest/gtest.h>

class SimpleDbInMemoryIndexTest : public ::testing::Test {
protected:
  const std::string filename = "test.db";
  SimpleDbInMemoryIndex *db;

  const nlohmann::json greeting_json = {{"hello", "world"}};
  const nlohmann::json menu_json = {{"breakfast", "bubur ayam"},
                                    {"lunch", "nasi rendang"},
                                    {"dinner", "nasi goreng"}};

  void SetUp() override {
    db = new SimpleDbInMemoryIndex(filename);
    db->set("greeting", greeting_json);
    db->set("menu", menu_json);
  }

  void TearDown() override {
    delete db;
    std::remove(filename.c_str());
  }
};

// Test case for existing database
TEST_F(SimpleDbInMemoryIndexTest, ExistingDb) {
  SimpleDbInMemoryIndex db2(filename);
  auto idx_map =
      db2.get_index().get_idx_map();
  std::unordered_map<std::string, std::pair<long, size_t>> expected_idx_map = {
      {"greeting", {0, 27}}, {"menu", {27, 78}}};
  EXPECT_EQ(idx_map, expected_idx_map);
  EXPECT_EQ(db2.get("menu"), menu_json);
  EXPECT_EQ(db2.get("greeting"), greeting_json);
}

// Test case for setting values
TEST_F(SimpleDbInMemoryIndexTest, Set) {
  std::ifstream file(db->get_filename());
  ASSERT_TRUE(file.is_open());
  std::string line;
  std::getline(file, line);
  EXPECT_EQ(line, "greeting,{\"hello\":\"world\"}");
  std::getline(file, line);
  EXPECT_EQ(line, "menu,{\"breakfast\":\"bubur ayam\",\"dinner\":\"nasi "
                  "goreng\",\"lunch\":\"nasi rendang\"}");
  file.close();
}

// Test case for getting values
TEST_F(SimpleDbInMemoryIndexTest, Get) {
  EXPECT_EQ(db->get("menu"), menu_json);
  EXPECT_EQ(db->get("greeting"), greeting_json);
}

// Test case for getting invalid key
TEST_F(SimpleDbInMemoryIndexTest, GetInvalidKey) {
  EXPECT_EQ(db->get("invalid key"), nullptr);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}