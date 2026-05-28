@echo off
setlocal enabledelayedexpansion

REM 指定不连续的 IP 列表（空格分隔）
set IP_LIST=192.168.20.130 192.168.20.82 192.168.20.183 192.168.20.98 192.168.20.46

for %%i in (%IP_LIST%) do (
    echo [!date! !time!] Launching connect_test with IP %%i

    for /L %%b in (1,1,8) do (
        set /a start_conn=^(%%b-1^)*2000+1
        set /a end_conn=start_conn+1999
        echo [!date! !time!] Client %%i: batch %%b starting connections !start_conn! to !end_conn!

        start /b .\cmake-build-release\connect_test.exe %%i 192.168.20.130 5999 2000
        timeout /t 1 /nobreak >nul
    )
)

echo [!date! !time!] All instances started.
pause