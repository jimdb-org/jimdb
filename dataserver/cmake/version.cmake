# config build date
find_program(DATE_BIN date)
if(NOT DATE_BIN)
	message(FATAL_ERROR "Date program is not found")
else()
    execute_process(COMMAND ${DATE_BIN} +"%Y/%m/%d %H:%M:%S"
        RESULT_VARIABLE RETVAL OUTPUT_VARIABLE
        BUILD_DATE OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()

# config buid type
if (BUILD_TYPE STREQUAL "")
    set(BUILD_TYPE "Relase")
endif()

# config git version
find_package(Git)
if(NOT GIT_FOUND)
    message(FATAL_ERROR "Git is not found.")
else()
    execute_process(COMMAND ${GIT_EXECUTABLE} describe --tags --dirty --always
     RESULT_VARIABLE RETVAL OUTPUT_VARIABLE
     GIT_DESC OUTPUT_STRIP_TRAILING_WHITESPACE
     WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})
endif()

CONFIGURE_FILE(${PROJECT_SOURCE_DIR}/cmake/version.h.in ${PROJECT_SOURCE_DIR}/src/server/version_gen.h)
