@echo off
python -m twine upload dist/* --repository testpypi --verbose
pause