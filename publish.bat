@echo off
python -m pip install --upgrade twine
python -m twine upload dist/* --verbose
pause