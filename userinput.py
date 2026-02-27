#!/usr/bin/env python3
"""
Interactive user input script for agent workflow
"""
print("\n" + "="*60)
print("请输入下一步指令 (输入 'stop' 停止):")
print("="*60)
user_input = input("> ").strip()

if user_input:
    print(f"\n收到指令: {user_input}")
else:
    print("\n未收到指令")
