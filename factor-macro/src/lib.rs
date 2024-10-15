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

    let param_num = match input.data {
        syn::Data::Struct(data) => match data.fields {
            syn::Fields::Named(fields) => fields.named.len(),
            syn::Fields::Unnamed(fields) => fields.unnamed.len(),
            syn::Fields::Unit => 0,
        },
        _ => panic!("Should only derive FactorBasefor structs"),
    };

    // Generate the implementation of the FactorBase trait
    let factor_base_impl = quote! {
        impl FactorBase for #name {
            #[inline]
            fn fac_name() -> ::std::sync::Arc<str> {
                #snake_name.into()
            }
        }
    };

    let debug_impl = if param_num >= 1 {
        quote! {
            impl ::std::fmt::Debug for #name {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    let param: Param = self.0.clone().into();
                    match param {
                        Param::None => write!(f, "{}", &Self::fac_name()),
                        param => write!(f, "{}_{:?}", &Self::fac_name(), param)
                    }
                }
            }
        }
    } else {
        quote! {
            impl ::std::fmt::Debug for #name {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    write!(f, "{}", &Self::fac_name())
                }
            }
        }
    };
    let expanded = quote! {
        #factor_base_impl
        #debug_impl
    };
    // Convert the expanded code back into a TokenStream
    TokenStream::from(expanded)
}

#[proc_macro_derive(FromParam)]
pub fn derive_from_param(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let input = parse_macro_input!(input as DeriveInput);

    // Get the name of the struct
    let name = input.ident;

    // Generate the implementation based on the struct fields
    let expanded = match input.data {
        syn::Data::Struct(data) => match data.fields {
            syn::Fields::Unnamed(fields) => {
                if fields.unnamed.len() >= 1 {
                    let field = fields.unnamed.first().unwrap();
                    let ty = &field.ty;
                    match ty {
                        syn::Type::Path(type_path) if type_path.path.is_ident("i32") => {
                            quote! {
                                impl From<Param> for #name {
                                    fn from(p: Param) -> Self {
                                        Self(p.as_i32())
                                    }
                                }
                            }
                        },
                        syn::Type::Path(type_path) if type_path.path.is_ident("i64") => {
                            quote! {
                                impl From<Param> for #name {
                                    fn from(p: Param) -> Self {
                                        Self(p.as_i64())
                                    }
                                }
                            }
                        },
                        syn::Type::Path(type_path) if type_path.path.is_ident("f32") => {
                            quote! {
                                impl From<Param> for #name {
                                    fn from(p: Param) -> Self {
                                        Self(p.as_f32())
                                    }
                                }
                            }
                        },
                        syn::Type::Path(type_path) if type_path.path.is_ident("f64") => {
                            quote! {
                                impl From<Param> for #name {
                                    fn from(p: Param) -> Self {
                                        Self(p.as_f64())
                                    }
                                }
                            }
                        },
                        syn::Type::Path(type_path) if type_path.path.is_ident("usize") => {
                            quote! {
                                impl From<Param> for #name {
                                    fn from(p: Param) -> Self {
                                        Self(p.as_usize())
                                    }
                                }
                            }
                        },
                        syn::Type::Path(type_path) if type_path.path.is_ident("String") => {
                            quote! {
                                impl From<Param> for #name {
                                    fn from(p: Param) -> Self {
                                        Self(p.as_str().to_string())
                                    }
                                }
                            }
                        },
                        syn::Type::Path(type_path) if type_path.path.is_ident("Param") => {
                            quote! {
                                impl From<Param> for #name {
                                    fn from(p: Param) -> Self {
                                        Self(p)
                                    }
                                }
                            }
                        },
                        syn::Type::Path(type_path) => {
                            let result = 'a: {
                                if let Some(segment) = type_path.path.segments.first() {
                                    if segment.ident == "Option" {
                                        if let syn::PathArguments::AngleBracketed(args) =
                                            &segment.arguments
                                        {
                                            if let Some(syn::GenericArgument::Type(
                                                syn::Type::Path(inner_type),
                                            )) = args.args.first()
                                            {
                                                if inner_type.path.is_ident("usize") {
                                                    break 'a quote! {
                                                        impl From<Param> for #name {
                                                            fn from(p: Param) -> Self {
                                                                match p {
                                                                    Param::None => Self(None),
                                                                    _ => Self(Some(p.as_usize()))
                                                                }
                                                            }
                                                        }
                                                    };
                                                }
                                            }
                                        }
                                    }
                                }
                                panic!("Invalid type to derive FromParam: {:?}", type_path)
                            };
                            result
                        },
                        _ => panic!("Invalid field type: {:?} to derive FromParam", field),
                    }
                } else {
                    quote! {
                        impl From<Param> for #name {
                            fn from(_p: Param) -> Self {
                                Self()
                            }
                        }
                    }
                }
            },
            syn::Fields::Unit => {
                quote! {
                    impl From<Param> for #name {
                        fn from(_: Param) -> Self {
                            Self
                        }
                    }
                }
            },
            _ => panic!("Invalid field type to derive FromParam"),
        },
        _ => panic!("Invalid field type to derive FromParam"),
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
