@echo off
if /I "%~1"=="--inside-console" goto :BOT_MAIN
start "AutoBot - Edge Console" "%ComSpec%" /D /K call "%~f0" --inside-console
if errorlevel 1 (
    echo [LOI] Khong mo duoc cua so CMD moi. Thu chay: cmd /k run.bat --inside-console
    pause
)
exit /b 0

:BOT_MAIN
setlocal EnableExtensions DisableDelayedExpansion
chcp 65001 >nul
set "BOT_DIR=%~dp0"
cd /d "%BOT_DIR%"
title AutoBot - Edge - CMD Keep Open

if not exist "logs" mkdir "logs" 2>nul
set "LAUNCH_LOG=%BOT_DIR%logs\launcher_cmd.log"
>>"%LAUNCH_LOG%" echo [%DATE% %TIME%] START launcher / Microsoft Edge configured profile

echo.
echo ==========================================================
echo   AUTOBOT - MICROSOFT EDGE - CMD LUON MO
echo   Edge: profile va cong CDP theo cau hinh - mot profile
echo ==========================================================
echo.

if not exist "main_script.py" (
    echo [LOI] Thieu main_script.py trong %BOT_DIR%
    set "BOT_EXIT=2"
    goto :BOT_END
)
if not exist ".env" (
    echo [LOI] Chua co .env. Hay giu file .env cu khi cap nhat.
    set "BOT_EXIT=2"
    goto :BOT_END
)

if exist ".venv312\Scripts\activate.bat" goto :USE_VENV312
if exist "venv\Scripts\activate.bat" goto :USE_VENV
echo [LOI] Chua co moi truong Python. Chay setup.bat truoc.
set "BOT_EXIT=2"
goto :BOT_END

:USE_VENV312
echo [*] Kich hoat .venv312...
call ".venv312\Scripts\activate.bat"
if errorlevel 1 goto :VENV_FAILED
goto :VENV_READY

:USE_VENV
echo [*] Kich hoat venv...
call "venv\Scripts\activate.bat"
if errorlevel 1 goto :VENV_FAILED
goto :VENV_READY

:VENV_FAILED
echo [LOI] Kich hoat Python venv that bai.
set "BOT_EXIT=2"
goto :BOT_END

:VENV_READY
set "PYTHONIOENCODING=utf-8"
set "PYTHONUTF8=1"
echo [*] AutoBot ket noi profile Edge qua CDP theo cau hinh.
echo [*] Dung bot cu bang Ctrl+C truoc; khong dung chung CDP voi Chrome.
echo [*] Chay AutoBot (tu chay lai neu bot chet bat ngo)...
if not defined AUTOBOT_AUTO_RESTART set "AUTOBOT_AUTO_RESTART=1"
if not defined AUTOBOT_MAX_RESTARTS set "AUTOBOT_MAX_RESTARTS=10"
if not defined AUTOBOT_STABLE_SECONDS set "AUTOBOT_STABLE_SECONDS=600"
set "AUTOBOT_RESTARTED=0"
set "RESTART_COUNT=0"

:RUN_BOT
call :NOW_SECONDS START_TS
>>"%LAUNCH_LOG%" echo [%DATE% %TIME%] starting main_script.py restart_count=%RESTART_COUNT%
python main_script.py
set "BOT_EXIT=%ERRORLEVEL%"
call :NOW_SECONDS END_TS
set /a "UPTIME=END_TS-START_TS"
if %UPTIME% LSS 0 set /a "UPTIME+=86400"
>>"%LAUNCH_LOG%" echo [%DATE% %TIME%] STOP exit_code=%BOT_EXIT% uptime=%UPTIME%s
set "STOP_LOGGED=1"

rem --- Khong chay lai: tat chu dong / loi cau hinh-khoi dong / ban khac chay chung session / auth key hong / Ctrl+C
if "%AUTOBOT_AUTO_RESTART%"=="0" goto :BOT_END
if "%BOT_EXIT%"=="0" goto :BOT_END
if "%BOT_EXIT%"=="1" goto :BOT_END
if "%BOT_EXIT%"=="2" goto :BOT_END
if "%BOT_EXIT%"=="3" goto :BOT_END
if "%BOT_EXIT%"=="78" goto :BOT_END
if "%BOT_EXIT%"=="-1073741510" goto :BOT_END

rem --- Con lai (4 = bot tu dung ngoai y muon, hoac crash) -> chay lai voi cho tang dan 5,10,20,...,120 giay
if %UPTIME% GEQ %AUTOBOT_STABLE_SECONDS% set "RESTART_COUNT=0"
set /a "RESTART_COUNT+=1"
if %RESTART_COUNT% GTR %AUTOBOT_MAX_RESTARTS% goto :RESTART_GIVEUP
set /a "WAIT=5"
set /a "I=1"
:DELAY_LOOP
if %I% GEQ %RESTART_COUNT% goto :DELAY_DONE
set /a "WAIT=WAIT*2"
set /a "I+=1"
goto :DELAY_LOOP
:DELAY_DONE
if %WAIT% GTR 120 set "WAIT=120"
echo.
echo [!] Bot dung ngoai y muon, exit code %BOT_EXIT%, chay duoc %UPTIME%s.
echo [!] Tu chay lai lan %RESTART_COUNT%/%AUTOBOT_MAX_RESTARTS% sau %WAIT%s. Bam Ctrl+C de huy.
>>"%LAUNCH_LOG%" echo [%DATE% %TIME%] RESTART %RESTART_COUNT%/%AUTOBOT_MAX_RESTARTS% sau %WAIT%s
set /a "PING_N=WAIT+1"
ping -n %PING_N% 127.0.0.1 >nul
set "AUTOBOT_RESTARTED=1"
goto :RUN_BOT

:RESTART_GIVEUP
echo [LOI] Da tu chay lai %AUTOBOT_MAX_RESTARTS% lan lien tiep van loi - dung han. Xem logs\bot_activity.log
>>"%LAUNCH_LOG%" echo [%DATE% %TIME%] GIVE UP sau %AUTOBOT_MAX_RESTARTS% lan chay lai lien tiep
goto :BOT_END

:BOT_END
if not defined BOT_EXIT set "BOT_EXIT=1"
if not defined STOP_LOGGED >>"%LAUNCH_LOG%" echo [%DATE% %TIME%] STOP exit_code=%BOT_EXIT%
echo.
echo ==========================================================
echo   Bot da dung hoac bi loi. Exit code: %BOT_EXIT%
if "%BOT_EXIT%"=="3" echo   Co ban bot khac dang chay cung session. Tat ban do (tasklist ^| findstr python) roi chay lai.
echo   Nhat ky launcher: "%LAUNCH_LOG%"
echo   CMD nay khong tu dong dong sau khi bot dung.
echo   Bam phim bat ky de tro ve dau nhac CMD.
echo ==========================================================
pause >nul
exit /b %BOT_EXIT%

:NOW_SECONDS
rem Doi gio hien tai thanh so giay trong ngay, tra vao bien ten %1
setlocal
set "T=%TIME: =0%"
set "S=0"
set /a "S=(1%T:~0,2%-100)*3600+(1%T:~3,2%-100)*60+(1%T:~6,2%-100)" 2>nul
endlocal & set "%~1=%S%"
exit /b 0
