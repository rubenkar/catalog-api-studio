@echo off
REM Standalone Bbox Detection Testbench
REM Usage: testbench <pdf_path> [page_number]
REM Example: testbench data\uploads\FBJ - General Catalogue.pdf 4

.venv\Scripts\python.exe testbench.py %*
