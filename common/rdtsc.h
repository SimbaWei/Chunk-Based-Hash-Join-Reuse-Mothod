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

/*
 * rdtsc is the assembly instruction that is used to
 * get the number of run cycles since CPU starts.
 */

#ifdef __cplusplus
extern "C" {
#endif

#if !defined(__i386__) && !defined(__x86_64__) && !defined(__sparc__)
#warning No supported architecture found -- timers will return junk.
#endif

static __inline__ unsigned long long curtick() {
    unsigned long long tick;
#if defined(__i386__)
    unsigned long lo, hi;
    __asm__ __volatile__ (".byte 0x0f, 0x31" : "=a" (lo), "=d" (hi));
    tick = (unsigned long long) hi << 32 | lo;
#elif defined(__x86_64__)
    unsigned long lo, hi;
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    tick = (unsigned long long) hi << 32 | lo;
#elif defined(__sparc__)
    __asm__ __volatile__ ("rd %%tick, %0" : "=r" (tick));
#endif
    return tick;
}

static __inline__ void startTimer(unsigned long long* t) {
    *t = curtick();
}

static __inline__ void stopTimer(unsigned long long* t) {
    *t = curtick() - *t;
}

#ifdef __cplusplus
}
#endif
