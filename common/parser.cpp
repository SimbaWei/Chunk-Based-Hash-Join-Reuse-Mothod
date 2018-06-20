/*
    Copyright 2018, Simba Wei.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/


#include "parser.h"

using namespace std;

int Parser::parse_line(char *line, const char **result)
{
    char* s = line; ///< pointer to the start of token
    char* p = line; ///< pointer to the end of token
    int ret = 0;

    while(*s)
    {
        while(*p != sep_ && *p)
        {
            p++;
        }
        if(s != p)   ///< eat null fields
            result[ret++] = s;
        if(*p)       ///< stopped because of sep
        {
            (*p) = 0;
            s = ++p;
        }
        else
        {
            break;
        }
    }
    return ret;
}
