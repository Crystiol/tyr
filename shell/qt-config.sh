#!/bin/sh
./configure \
-extprefix /home/tuxu/qt5.15.12-aarch64 \
-confirm-license \
-opensource \
-release \
-make libs \
-xplatform linux-aarch64-gnu-g++ \
-pch \
-qt-libjpeg \
-qt-libpng \
-qt-zlib \
-no-sse2 \
-no-openssl \
-no-cups \
-no-glib \
-no-dbus \
-qt-xcb \
-no-separate-debug-info \
-opengl es2 \
-egl \
-eglfs \
-qpa eglfs \
-no-linuxfb \
-skip location \
-recheck-all

