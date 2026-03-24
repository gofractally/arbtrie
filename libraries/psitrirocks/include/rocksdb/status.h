#pragma once
#include <string>

namespace rocksdb
{

   class Status
   {
     public:
      Status() : code_(kOk) {}

      static Status OK() { return Status(); }
      static Status NotFound(const std::string& msg = "")
      {
         return Status(kNotFound, msg);
      }
      static Status Corruption(const std::string& msg = "")
      {
         return Status(kCorruption, msg);
      }
      static Status NotSupported(const std::string& msg = "")
      {
         return Status(kNotSupported, msg);
      }
      static Status InvalidArgument(const std::string& msg = "")
      {
         return Status(kInvalidArgument, msg);
      }
      static Status IOError(const std::string& msg = "")
      {
         return Status(kIOError, msg);
      }
      static Status Incomplete(const std::string& msg = "")
      {
         return Status(kIncomplete, msg);
      }

      bool ok() const { return code_ == kOk; }
      bool IsNotFound() const { return code_ == kNotFound; }
      bool IsCorruption() const { return code_ == kCorruption; }
      bool IsNotSupported() const { return code_ == kNotSupported; }
      bool IsInvalidArgument() const { return code_ == kInvalidArgument; }
      bool IsIOError() const { return code_ == kIOError; }
      bool IsIncomplete() const { return code_ == kIncomplete; }

      std::string ToString() const
      {
         switch (code_)
         {
            case kOk:
               return "OK";
            case kNotFound:
               return "NotFound: " + msg_;
            case kCorruption:
               return "Corruption: " + msg_;
            case kNotSupported:
               return "Not implemented: " + msg_;
            case kInvalidArgument:
               return "Invalid argument: " + msg_;
            case kIOError:
               return "IO error: " + msg_;
            case kIncomplete:
               return "Incomplete: " + msg_;
            default:
               return "Unknown status";
         }
      }

      // Allow bool conversion for simple ok() checks
      explicit operator bool() const { return ok(); }

     private:
      enum Code
      {
         kOk = 0,
         kNotFound,
         kCorruption,
         kNotSupported,
         kInvalidArgument,
         kIOError,
         kIncomplete,
      };

      Status(Code code, const std::string& msg = "") : code_(code), msg_(msg) {}

      Code        code_;
      std::string msg_;
   };

}  // namespace rocksdb
