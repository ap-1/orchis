//! Compile-fail tests verifying the typestate builder rejects invalid pipelines.

#[test]
fn compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
}
