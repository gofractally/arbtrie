#pragma once
#include <type_traits>

namespace ucc
{
   /**
    * copies the constness of the src type to the destionation type
    */
   template <class Src, class Dst>
   using transcribe_const_t = std::conditional_t<std::is_const<Src>{}, const Dst, Dst>;
   /**
    * copies the volatileness of the src type to the destionation type
    */
   using transcribe_volatile_t = std::conditional_t<std::is_volatile<Src>{}, Dst volatile, Dst>;
   /**
    * copies the constness and volatileness of the src type to the destionation type
    */
   template <class Src, class Dst>
   using transcribe_cv_t = transcribe_const_t<Src, transcribe_volatile_t<Src, Dst> >;
}  // namespace ucc
