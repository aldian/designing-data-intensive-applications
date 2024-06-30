TARGET = test_${APP}
SOURCES = ${APP}.cpp test_${APP}.cpp

OBJECTS = $(SOURCES:.cpp=.o)

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CXX) $(OBJECTS) -o $(TARGET) $(LDFLAGS)

%.o: %.cpp %.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJECTS) $(TARGET) *.gcda *.gcno coverage.info
	rm -rf out

coverage: all run_tests generate_report

run_tests: $(TARGET)
	./$(TARGET)

generate_report:
	lcov --capture --directory . --output-file coverage.info --no-external --rc lcov_branch_coverage=1
	lcov --remove coverage.info '/usr/*' '*test_*' --output-file coverage.info
	genhtml coverage.info --output-directory out
	@echo "Coverage report generated in out/index.html"

format:
	clang-format -i *.cpp *.h