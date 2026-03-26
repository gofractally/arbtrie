#pragma once
#include <atomic>
#include <csignal>
#include <cstdio>
#include <unistd.h>

/**
 * Shared SIGINT/SIGTERM handler for benchmark programs.
 *
 * Call install_interrupt_handler() at the start of main().
 * Check interrupted() in hot loops to exit gracefully,
 * allowing DB destructors to run and flush data to disk.
 */

namespace bench
{
   inline std::atomic<bool> g_interrupted{false};

   inline bool interrupted() noexcept
   {
      return g_interrupted.load(std::memory_order_relaxed);
   }

   inline void signal_handler(int sig)
   {
      if (g_interrupted.load(std::memory_order_relaxed))
      {
         // Second signal — force exit
         fprintf(stderr, "\nForced exit.\n");
         _exit(128 + sig);
      }
      g_interrupted.store(true, std::memory_order_relaxed);
      fprintf(stderr, "\nInterrupted — shutting down gracefully...\n");
   }

   inline void install_interrupt_handler()
   {
      struct sigaction sa{};
      sa.sa_handler = signal_handler;
      sa.sa_flags   = 0;  // no SA_RESTART — interrupt blocking calls
      sigemptyset(&sa.sa_mask);
      sigaction(SIGINT, &sa, nullptr);
      sigaction(SIGTERM, &sa, nullptr);
   }
}  // namespace bench
