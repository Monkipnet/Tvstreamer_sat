option(TVSTREAMMERSAT5_BUILD_OSCAM_MINI "Build vendored OSCam-mini" ON)

if(TVSTREAMMERSAT5_BUILD_OSCAM_MINI)
  set(OSCAM_MINI_SOURCE_DIR "${CMAKE_SOURCE_DIR}/third_party/oscam-mini")
  set(OSCAM_MINI_OUTPUT_DIR "${CMAKE_BINARY_DIR}/oscam-mini")
  set(OSCAM_MINI_BINARY "${OSCAM_MINI_OUTPUT_DIR}/oscam-mini")

  if(NOT EXISTS "${OSCAM_MINI_SOURCE_DIR}/config.sh" OR
     NOT EXISTS "${OSCAM_MINI_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR
      "Vendored OSCam source is incomplete in ${OSCAM_MINI_SOURCE_DIR}. "
      "The repository must contain third_party/oscam-mini before building.")
  endif()

  add_custom_command(
    OUTPUT "${OSCAM_MINI_BINARY}"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${OSCAM_MINI_OUTPUT_DIR}"
    # Invoke via bash deliberately: Windows ZIP extraction can lose the executable bit.
    COMMAND /usr/bin/env bash
            "${CMAKE_SOURCE_DIR}/scripts/build_oscam_mini.sh"
            "${OSCAM_MINI_SOURCE_DIR}"
            "${OSCAM_MINI_OUTPUT_DIR}"
    DEPENDS
      "${CMAKE_SOURCE_DIR}/scripts/build_oscam_mini.sh"
      "${OSCAM_MINI_SOURCE_DIR}/config.sh"
      "${OSCAM_MINI_SOURCE_DIR}/CMakeLists.txt"
    COMMENT "Building vendored OSCam-mini (Newcamd + Phoenix + PC/SC + compiled CA reader families)"
    USES_TERMINAL
    VERBATIM)

  add_custom_target(oscam-mini ALL DEPENDS "${OSCAM_MINI_BINARY}")

  install(PROGRAMS "${OSCAM_MINI_BINARY}"
          DESTINATION /opt/TVStreammerSAT5/oscam-mini)
  install(PROGRAMS "${CMAKE_SOURCE_DIR}/scripts/install_oscam_mini.sh"
          DESTINATION /opt/TVStreammerSAT5/oscam-mini)
  install(FILES "${CMAKE_SOURCE_DIR}/packaging/oscam-mini/oscam-mini.service"
          DESTINATION /opt/TVStreammerSAT5/oscam-mini)
  # Install the unit itself as part of cmake --install. The helper below only
  # reloads systemd, disables legacy OSCam and enables/starts this service.
  install(FILES "${CMAKE_SOURCE_DIR}/packaging/oscam-mini/oscam-mini.service"
          DESTINATION /etc/systemd/system)
  install(DIRECTORY "${CMAKE_SOURCE_DIR}/packaging/oscam-mini/default-config/"
          DESTINATION /opt/TVStreammerSAT5/oscam-mini/default-config)
endif()
