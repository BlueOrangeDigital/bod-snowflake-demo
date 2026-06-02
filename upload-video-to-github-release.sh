#!/bin/bash
WEBM=$(ls recordings/*.webm | head -1)
gh release create v1.0-demo \
  --title "Snowflake AI & Cortex Demo — v1.0" \
  --notes "$(cat recordings/chapters.txt)" \
  "$WEBM" \
  recordings/chapters.txt