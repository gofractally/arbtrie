#pragma once
#include <utility>

namespace psitri
{
   /**
    * RAII utility that executes a cleanup function when going out of scope
    */
   template <typename F>
   class scoped_exit
   {
      F    _cleanup;
      bool _active = true;  // Flag to track if this instance owns the cleanup responsibility

     public:
      explicit scoped_exit(F&& cleanup) : _cleanup(std::move(cleanup)) {}
      ~scoped_exit()
      {
         if (_active)
            _cleanup();
      }

      // Prevent copying
      scoped_exit(const scoped_exit&)            = delete;
      scoped_exit& operator=(const scoped_exit&) = delete;

      // Allow moving
      scoped_exit(scoped_exit&& other) noexcept
          : _cleanup(std::move(other._cleanup)), _active(other._active)
      {
         other._active = false;  // Transfer ownership
      }

      scoped_exit& operator=(scoped_exit&& other) noexcept
      {
         if (this != &other)
         {
            if (_active)
               _cleanup();  // Call cleanup of current object before replacing it

            _cleanup      = std::move(other._cleanup);
            _active       = other._active;
            other._active = false;  // Transfer ownership
         }
         return *this;
      }
   };

   // Deduction guide
   template <typename F>
   scoped_exit(F&&) -> scoped_exit<F>;

}  // namespace psitri
