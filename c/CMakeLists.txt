cmake_minimum_required(VERSION 3.10)

project(banktivity-stock-quote-sync-c)

add_library(log log.c)

add_executable(ibdq main.c)

target_link_libraries(ibdq PUBLIC curl)
target_link_libraries(ibdq PUBLIC log)
target_link_libraries(ibdq PUBLIC sqlite3)

target_include_directories(ibdq PUBLIC
                          "${PROJECT_BINARY_DIR}"
                          "${PROJECT_SOURCE_DIR}"
                          )
