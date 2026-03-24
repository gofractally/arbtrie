#pragma once
#include <cstdint>
#include <string>

namespace rocksdb
{

   class Env
   {
     public:
      virtual ~Env() = default;

      static Env* Default()
      {
         static Env env;
         return &env;
      }

      virtual uint64_t NowMicros() const
      {
         struct timespec ts;
         clock_gettime(CLOCK_MONOTONIC, &ts);
         return static_cast<uint64_t>(ts.tv_sec) * 1000000 + ts.tv_nsec / 1000;
      }

      virtual uint64_t NowNanos() const
      {
         struct timespec ts;
         clock_gettime(CLOCK_MONOTONIC, &ts);
         return static_cast<uint64_t>(ts.tv_sec) * 1000000000 + ts.tv_nsec;
      }
   };

}  // namespace rocksdb
