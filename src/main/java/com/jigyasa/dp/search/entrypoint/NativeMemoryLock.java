package com.jigyasa.dp.search.entrypoint;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;
import com.sun.jna.platform.win32.BaseTSD.SIZE_T;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinNT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Native memory locking via JNA — pins JVM heap into physical RAM,
 * preventing the OS from swapping it out. This eliminates GC pauses
 * caused by swapped-out heap pages (which can spike p99 from ms to seconds).
 *
 * Mirrors Elasticsearch's bootstrap.memory_lock implementation:
 * - Linux/macOS: mlockall(MCL_CURRENT | MCL_FUTURE) via libc
 * - Windows: SetProcessWorkingSetSize + VirtualLock via kernel32
 *
 * Requires:
 * - Linux: ulimit -l unlimited
 * - Windows: "Lock pages in memory" privilege (Local Security Policy → User Rights Assignment)
 */
final class NativeMemoryLock {
    private static final Logger log = LoggerFactory.getLogger(NativeMemoryLock.class);

    private NativeMemoryLock() {}

    private static final boolean IS_WINDOWS = System.getProperty("os.name", "").toLowerCase().contains("win");
    private static final boolean IS_MAC = System.getProperty("os.name", "").toLowerCase().contains("mac");

    // Linux/macOS libc constants
    static final int MCL_CURRENT = 1;
    static final int MCL_FUTURE = 2;
    static final int ENOMEM = 12;
    static final int RLIMIT_MEMLOCK = IS_MAC ? 6 : 8;
    static final long RLIM_INFINITY = IS_MAC ? Long.MAX_VALUE : -1L;

    // Windows memory state/protection constants
    static final int MEM_COMMIT = 0x1000;
    static final int PAGE_NOACCESS = 0x01;
    static final int PAGE_GUARD = 0x100;

    private static boolean libcLoaded = false;

    static {
        if (!IS_WINDOWS) {
            try {
                Native.register("c");
                libcLoaded = true;
            } catch (UnsatisfiedLinkError e) {
                log.debug("Unable to link libc via JNA: {}", e.getMessage());
            }
        }
    }

    // Native libc bindings (Linux/macOS only)
    static native int mlockall(int flags);
    static native String strerror(int errno);
    static native int getrlimit(int resource, Rlimit rlimit);

    @Structure.FieldOrder({"rlim_cur", "rlim_max"})
    public static class Rlimit extends Structure implements Structure.ByReference {
        public NativeLong rlim_cur = new NativeLong(0);
        public NativeLong rlim_max = new NativeLong(0);
    }

    /**
     * Attempt to lock all memory pages into physical RAM.
     * Dispatches to mlockall() on Linux/macOS or VirtualLock on Windows.
     * @return true if memory was successfully locked
     */
    static boolean tryLockMemory() {
        if (IS_WINDOWS) {
            return tryVirtualLock();
        } else {
            return tryMlockall();
        }
    }

    // ---- Linux/macOS: mlockall ----

    private static boolean tryMlockall() {
        if (!libcLoaded) {
            log.warn("JNA libc not available — cannot call mlockall. Use -XX:+AlwaysPreTouch as fallback.");
            return false;
        }

        try {
            int result = mlockall(MCL_CURRENT | MCL_FUTURE);
            if (result == 0) {
                log.info("Memory locked via mlockall() — JVM heap pinned to physical RAM");
                return true;
            }

            int errno = Native.getLastError();
            String errMsg = strerror(errno);
            log.warn("mlockall() failed: errno={}, reason={}", errno, errMsg);

            if (errno == ENOMEM) {
                Rlimit rlimit = new Rlimit();
                if (getrlimit(RLIMIT_MEMLOCK, rlimit) == 0) {
                    long softLimit = rlimit.rlim_cur.longValue();
                    long hardLimit = rlimit.rlim_max.longValue();
                    log.warn("RLIMIT_MEMLOCK: soft={}, hard={}",
                            rlimitToString(softLimit), rlimitToString(hardLimit));
                    String user = System.getProperty("user.name");
                    log.warn("Fix: add to /etc/security/limits.conf:\n" +
                            "  {} soft memlock unlimited\n" +
                            "  {} hard memlock unlimited", user, user);
                }
            }
            return false;
        } catch (UnsatisfiedLinkError e) {
            log.warn("mlockall not available: {}", e.getMessage());
            return false;
        }
    }

    // ---- Windows: VirtualLock ----

    /**
     * Windows equivalent of mlockall: expand the process working set to fit the JVM heap,
     * then iterate all committed memory regions and VirtualLock each one.
     * Mirrors Elasticsearch's JNANatives.tryVirtualLock().
     */
    private static boolean tryVirtualLock() {
        try {
            Kernel32 kernel32 = Kernel32.INSTANCE;
            WinNT.HANDLE process = kernel32.GetCurrentProcess();

            // Iterate all memory regions and VirtualLock committed, accessible pages.
            // Windows VirtualLock pins individual memory regions into physical RAM.
            Pointer address = new Pointer(0);
            WinNT.MEMORY_BASIC_INFORMATION memInfo = new WinNT.MEMORY_BASIC_INFORMATION();
            int lockedRegions = 0;
            long lockedBytes = 0;

            while (Kernel32.INSTANCE.VirtualQueryEx(process, address, memInfo, new SIZE_T(memInfo.size())).longValue() != 0) {
                boolean committed = memInfo.state.longValue() == MEM_COMMIT;
                boolean accessible = (memInfo.protect.longValue() & PAGE_NOACCESS) == 0
                        && (memInfo.protect.longValue() & PAGE_GUARD) == 0;

                if (committed && accessible) {
                    boolean locked = kernel32.VirtualLock(memInfo.baseAddress, new SIZE_T(memInfo.regionSize.longValue()));
                    if (locked) {
                        lockedRegions++;
                        lockedBytes += memInfo.regionSize.longValue();
                    }
                }

                long nextAddr = Pointer.nativeValue(memInfo.baseAddress) + memInfo.regionSize.longValue();
                if (nextAddr <= Pointer.nativeValue(address)) break;
                address = new Pointer(nextAddr);
            }

            if (lockedRegions > 0) {
                log.info("Memory locked via VirtualLock — {} regions, {}MB pinned to physical RAM",
                        lockedRegions, lockedBytes / (1024 * 1024));
                return true;
            } else {
                log.warn("VirtualLock: no regions locked. Ensure 'Lock pages in memory' privilege is granted " +
                        "(Local Security Policy → User Rights Assignment → Lock pages in memory).");
                return false;
            }
        } catch (UnsatisfiedLinkError e) {
            log.warn("kernel32 VirtualLock not available: {}", e.getMessage());
            return false;
        } catch (Exception e) {
            log.warn("VirtualLock failed: {}", e.getMessage());
            return false;
        }
    }

    private static String rlimitToString(long value) {
        if (value == RLIM_INFINITY || Long.toUnsignedString(value).equals(Long.toUnsignedString(-1L))) {
            return "unlimited";
        }
        return Long.toUnsignedString(value);
    }
}
