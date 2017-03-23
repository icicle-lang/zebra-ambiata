#if __clang__
    #pragma clang diagnostic ignored "-Wformat"
    #pragma clang diagnostic ignored "-Wunused-variable"
#elif __GNUC__
    #pragma GCC diagnostic ignored "-Wformat"
#endif

#if __GLASGOW_HASKELL__ >= 800
    #define bc_zpatsig(name,constr) \
        printf("pattern ");bc_conid(name); \
        printf(" :: (Eq a, %s a) => a",constr);
#else
    #define bc_zpatsig(name,constr)
#endif

#define hsc_znum(name) \
    bc_zpatsig(# name,"Num");printf("\n"); \
    printf("pattern ");bc_conid(# name);printf(" <- ((== ("); \
    bc_decimal(name);printf(")) -> True) where\n    "); \
    bc_conid(# name);printf(" = ");bc_decimal(name);
