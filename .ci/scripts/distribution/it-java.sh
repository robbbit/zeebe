#!/bin/sh -eux


export JAVA_TOOL_OPTIONS="$JAVA_TOOL_OPTIONS -XX:MaxRAMFraction=$((LIMITS_CPU))"

mvn -o -B -T$LIMITS_CPU -s ${MAVEN_SETTINGS_XML} verify -P skip-unstable-ci,parallel-tests -pl qa/integration-tests -pl upgrade-tests -DtestMavenId=2  -Dsurefire.reportNameSuffix=it
