#include <cstdlib>
#include <cwchar>
#include <memory>
#include <stdlib.h>

#include "util.h"

int getStrWidth(const char* s) {
    int wchar_len = std::mbstowcs(NULL, s, 0);
    std::unique_ptr<wchar_t[]>  wchar_str(new wchar_t[wchar_len]);
    if (std::mbstowcs(wchar_str.get(), s, wchar_len)==-1){
        return -1;
    }
    int width =0;
    for (int i =0; i < wchar_len; i++) {
        int w = wcwidth(wchar_str.get()[i]);
        if (w>0) {
            width+=w;
        }
    }
    return width;
}

void formatTime(char* buf, int size, struct tm* t) {
    strftime(buf, 255, "%Y-%m-%d %H:%M:%S", t);
}