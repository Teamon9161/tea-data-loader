extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(FactorBase)]
pub fn derive_factor_base(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    // Get the name of the struct
    let name = input.ident;

    // Generate the implementation of the FactorBase trait
    let expanded = quote! {
        impl FactorBase for #name {
            #[inline]
            fn fac_name() -> ::std::sync::Arc<str> {
                stringify!(#name).to_lowercase().into()
            }

            #[inline]
            fn new<P: Into<Param>>(param: P) -> Self {
                Self(param.into())
            }
        }

        impl GetFacName for #name {
            #[inline]
            fn name(&self) -> String {
                match &self.0 {
                    Param::None => format!("{}", &Self::fac_name()),
                    param => format!("{}_{:?}", &Self::fac_name(), param)
                }
            }
        }
    };

    // Convert the expanded code back into a TokenStream
    TokenStream::from(expanded)
}
