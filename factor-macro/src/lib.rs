extern crate proc_macro;
mod utils;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};
use utils::to_snake_case;

#[proc_macro_derive(FactorBase)]
pub fn derive_factor_base(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    // Get the name of the struct
    let name = input.ident;

    let snake_name = to_snake_case(&name.to_string());

    // Generate the implementation of the FactorBase trait
    let expanded = quote! {
        impl FactorBase for #name {
            #[inline]
            fn fac_name() -> ::std::sync::Arc<str> {
                #snake_name.into()
            }

            #[inline]
            fn new(param: impl Into<Param>) -> Self {
                Self(param.into())
            }
        }

        impl ::std::fmt::Debug for #name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                match self.0 {
                    Param::None => write!(f, "{}", &Self::fac_name()),
                    param => write!(f, "{}_{:?}", &Self::fac_name(), param)
                }
            }
        }
    };

    // Convert the expanded code back into a TokenStream
    TokenStream::from(expanded)
}

#[proc_macro_derive(StrategyBase)]
pub fn derive_strategy_base(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    // Get the name of the struct
    let name = input.ident;

    let snake_name = to_snake_case(&name.to_string());

    // Generate the implementation of the FactorBase trait
    let expanded = quote! {
        impl StrategyBase for #name {
            #[inline]
            fn strategy_name() -> ::std::sync::Arc<str> {
                #snake_name.into()
            }
        }

        impl ::std::fmt::Debug for #name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                write!(f, "{}_{:?}", &Self::strategy_name(), self.get_param_name())
            }
        }

        impl GetName for #name {}
    };

    // Convert the expanded code back into a TokenStream
    TokenStream::from(expanded)
}
