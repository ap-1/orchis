//! Proc macros for orchis task definitions.
//!
//! # Example
//!
//! ```ignore
//! use orchis_macros::task;
//!
//! #[task("processor::extract_content")]
//! async fn extract_content(input: Page) -> PageContent {
//!     // ...
//! }
//! ```
//!
//! Expands to:
//!
//! ```ignore
//! async fn extract_content(input: Page) -> PageContent {
//!     // ...
//! }
//!
//! struct ExtractContentTask;
//! impl orchis_core::Task for ExtractContentTask {
//!     type Input = Page;
//!     type Output = PageContent;
//!     const NAME: &'static str = "processor::extract_content";
//! }
//! ```

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, LitStr, ReturnType, parse_macro_input};

/// Derive a `Task` impl from an async function signature.
///
/// The attribute argument is the task name used for dispatch routing.
/// The function must have exactly one parameter (the input type) and
/// a return type (the output type).
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let task_name = parse_macro_input!(attr as LitStr);
    let func = parse_macro_input!(item as ItemFn);

    let fn_name = &func.sig.ident;

    let struct_name = format_ident!(
        "{}Task",
        fn_name
            .to_string()
            .split('_')
            .map(|s| {
                let mut c = s.chars();
                match c.next() {
                    None => String::new(),
                    Some(f) => f.to_uppercase().to_string() + c.as_str(),
                }
            })
            .collect::<String>()
    );

    let input_ty = match func.sig.inputs.first() {
        Some(syn::FnArg::Typed(pat_type)) => &pat_type.ty,
        _ => {
            return syn::Error::new_spanned(
                &func.sig,
                "task function must have exactly one parameter",
            )
            .to_compile_error()
            .into();
        }
    };

    if func.sig.inputs.len() != 1 {
        return syn::Error::new_spanned(&func.sig, "task function must have exactly one parameter")
            .to_compile_error()
            .into();
    }

    let output_ty = match &func.sig.output {
        ReturnType::Type(_, ty) => ty,
        ReturnType::Default => {
            return syn::Error::new_spanned(&func.sig, "task function must have a return type")
                .to_compile_error()
                .into();
        }
    };

    let vis = &func.vis;
    let expanded = quote! {
        #func

        #vis struct #struct_name;

        impl orchis_core::Task for #struct_name {
            type Input = #input_ty;
            type Output = #output_ty;
            const NAME: &'static str = #task_name;
        }
    };

    expanded.into()
}
