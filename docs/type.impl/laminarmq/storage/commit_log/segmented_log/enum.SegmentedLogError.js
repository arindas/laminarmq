(function() {var type_impls = {
"laminarmq":[["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-Debug-for-SegmentedLogError%3CSE,+SDE,+CE%3E\" class=\"impl\"><a class=\"src rightside\" href=\"src/laminarmq/storage/commit_log/segmented_log/mod.rs.html#165\">source</a><a href=\"#impl-Debug-for-SegmentedLogError%3CSE,+SDE,+CE%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;SE: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.78.0/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a>, SDE: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.78.0/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a>, CE: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.78.0/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a>&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/1.78.0/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a> for <a class=\"enum\" href=\"laminarmq/storage/commit_log/segmented_log/enum.SegmentedLogError.html\" title=\"enum laminarmq::storage::commit_log::segmented_log::SegmentedLogError\">SegmentedLogError</a>&lt;SE, SDE, CE&gt;</h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.fmt\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"src/laminarmq/storage/commit_log/segmented_log/mod.rs.html#165\">source</a><a href=\"#method.fmt\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/1.78.0/core/fmt/trait.Debug.html#tymethod.fmt\" class=\"fn\">fmt</a>(&amp;self, f: &amp;mut <a class=\"struct\" href=\"https://doc.rust-lang.org/1.78.0/core/fmt/struct.Formatter.html\" title=\"struct core::fmt::Formatter\">Formatter</a>&lt;'_&gt;) -&gt; <a class=\"type\" href=\"https://doc.rust-lang.org/1.78.0/core/fmt/type.Result.html\" title=\"type core::fmt::Result\">Result</a></h4></section></summary><div class='docblock'>Formats the value using the given formatter. <a href=\"https://doc.rust-lang.org/1.78.0/core/fmt/trait.Debug.html#tymethod.fmt\">Read more</a></div></details></div></details>","Debug","laminarmq::storage::commit_log::segmented_log::LogError"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-Display-for-SegmentedLogError%3CSE,+SDE,+CE%3E\" class=\"impl\"><a class=\"src rightside\" href=\"src/laminarmq/storage/commit_log/segmented_log/mod.rs.html#194-203\">source</a><a href=\"#impl-Display-for-SegmentedLogError%3CSE,+SDE,+CE%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;SE, SDE, CE&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/1.78.0/core/fmt/trait.Display.html\" title=\"trait core::fmt::Display\">Display</a> for <a class=\"enum\" href=\"laminarmq/storage/commit_log/segmented_log/enum.SegmentedLogError.html\" title=\"enum laminarmq::storage::commit_log::segmented_log::SegmentedLogError\">SegmentedLogError</a>&lt;SE, SDE, CE&gt;<div class=\"where\">where\n    SE: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.78.0/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a>,\n    SDE: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.78.0/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a>,\n    CE: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.78.0/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a>,</div></h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.fmt\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"src/laminarmq/storage/commit_log/segmented_log/mod.rs.html#200-202\">source</a><a href=\"#method.fmt\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/1.78.0/core/fmt/trait.Display.html#tymethod.fmt\" class=\"fn\">fmt</a>(&amp;self, f: &amp;mut <a class=\"struct\" href=\"https://doc.rust-lang.org/1.78.0/core/fmt/struct.Formatter.html\" title=\"struct core::fmt::Formatter\">Formatter</a>&lt;'_&gt;) -&gt; <a class=\"type\" href=\"https://doc.rust-lang.org/1.78.0/core/fmt/type.Result.html\" title=\"type core::fmt::Result\">Result</a></h4></section></summary><div class='docblock'>Formats the value using the given formatter. <a href=\"https://doc.rust-lang.org/1.78.0/core/fmt/trait.Display.html#tymethod.fmt\">Read more</a></div></details></div></details>","Display","laminarmq::storage::commit_log::segmented_log::LogError"],["<details class=\"toggle implementors-toggle\" open><summary><section id=\"impl-Error-for-SegmentedLogError%3CSE,+SDE,+CE%3E\" class=\"impl\"><a class=\"src rightside\" href=\"src/laminarmq/storage/commit_log/segmented_log/mod.rs.html#205-211\">source</a><a href=\"#impl-Error-for-SegmentedLogError%3CSE,+SDE,+CE%3E\" class=\"anchor\">§</a><h3 class=\"code-header\">impl&lt;SE, SDE, CE&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/1.78.0/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a> for <a class=\"enum\" href=\"laminarmq/storage/commit_log/segmented_log/enum.SegmentedLogError.html\" title=\"enum laminarmq::storage::commit_log::segmented_log::SegmentedLogError\">SegmentedLogError</a>&lt;SE, SDE, CE&gt;<div class=\"where\">where\n    SE: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.78.0/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a>,\n    SDE: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.78.0/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a>,\n    CE: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.78.0/core/fmt/trait.Debug.html\" title=\"trait core::fmt::Debug\">Debug</a>,</div></h3></section></summary><div class=\"impl-items\"><details class=\"toggle method-toggle\" open><summary><section id=\"method.source\" class=\"method trait-impl\"><span class=\"rightside\"><span class=\"since\" title=\"Stable since Rust version 1.30.0\">1.30.0</span> · <a class=\"src\" href=\"https://doc.rust-lang.org/1.78.0/src/core/error.rs.html#84\">source</a></span><a href=\"#method.source\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/1.78.0/core/error/trait.Error.html#method.source\" class=\"fn\">source</a>(&amp;self) -&gt; <a class=\"enum\" href=\"https://doc.rust-lang.org/1.78.0/core/option/enum.Option.html\" title=\"enum core::option::Option\">Option</a>&lt;&amp;(dyn <a class=\"trait\" href=\"https://doc.rust-lang.org/1.78.0/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a> + 'static)&gt;</h4></section></summary><div class='docblock'>The lower-level source of this error, if any. <a href=\"https://doc.rust-lang.org/1.78.0/core/error/trait.Error.html#method.source\">Read more</a></div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.description\" class=\"method trait-impl\"><span class=\"rightside\"><span class=\"since\" title=\"Stable since Rust version 1.0.0\">1.0.0</span> · <a class=\"src\" href=\"https://doc.rust-lang.org/1.78.0/src/core/error.rs.html#110\">source</a></span><a href=\"#method.description\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/1.78.0/core/error/trait.Error.html#method.description\" class=\"fn\">description</a>(&amp;self) -&gt; &amp;<a class=\"primitive\" href=\"https://doc.rust-lang.org/1.78.0/std/primitive.str.html\">str</a></h4></section></summary><span class=\"item-info\"><div class=\"stab deprecated\"><span class=\"emoji\">👎</span><span>Deprecated since 1.42.0: use the Display impl or to_string()</span></div></span><div class='docblock'> <a href=\"https://doc.rust-lang.org/1.78.0/core/error/trait.Error.html#method.description\">Read more</a></div></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.cause\" class=\"method trait-impl\"><span class=\"rightside\"><span class=\"since\" title=\"Stable since Rust version 1.0.0\">1.0.0</span> · <a class=\"src\" href=\"https://doc.rust-lang.org/1.78.0/src/core/error.rs.html#120\">source</a></span><a href=\"#method.cause\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/1.78.0/core/error/trait.Error.html#method.cause\" class=\"fn\">cause</a>(&amp;self) -&gt; <a class=\"enum\" href=\"https://doc.rust-lang.org/1.78.0/core/option/enum.Option.html\" title=\"enum core::option::Option\">Option</a>&lt;&amp;dyn <a class=\"trait\" href=\"https://doc.rust-lang.org/1.78.0/core/error/trait.Error.html\" title=\"trait core::error::Error\">Error</a>&gt;</h4></section></summary><span class=\"item-info\"><div class=\"stab deprecated\"><span class=\"emoji\">👎</span><span>Deprecated since 1.33.0: replaced by Error::source, which can support downcasting</span></div></span></details><details class=\"toggle method-toggle\" open><summary><section id=\"method.provide\" class=\"method trait-impl\"><a class=\"src rightside\" href=\"https://doc.rust-lang.org/1.78.0/src/core/error.rs.html#184\">source</a><a href=\"#method.provide\" class=\"anchor\">§</a><h4 class=\"code-header\">fn <a href=\"https://doc.rust-lang.org/1.78.0/core/error/trait.Error.html#method.provide\" class=\"fn\">provide</a>&lt;'a&gt;(&amp;'a self, request: &amp;mut <a class=\"struct\" href=\"https://doc.rust-lang.org/1.78.0/core/error/struct.Request.html\" title=\"struct core::error::Request\">Request</a>&lt;'a&gt;)</h4></section></summary><span class=\"item-info\"><div class=\"stab unstable\"><span class=\"emoji\">🔬</span><span>This is a nightly-only experimental API. (<code>error_generic_member_access</code>)</span></div></span><div class='docblock'>Provides type based access to context intended for error reports. <a href=\"https://doc.rust-lang.org/1.78.0/core/error/trait.Error.html#method.provide\">Read more</a></div></details></div></details>","Error","laminarmq::storage::commit_log::segmented_log::LogError"]]
};if (window.register_type_impls) {window.register_type_impls(type_impls);} else {window.pending_type_impls = type_impls;}})()