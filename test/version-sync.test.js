// @zakkster/lite-query -- runtime version sync guard (Q-16, OR-6).
//
// The VERSION const in Query.js is the single runtime source of this package's
// version. This test asserts it equals package.json's version -- a two-place
// sync (VERSION const == package.json). The CHANGELOG-head leg is owned by the
// /release drill (OR-6), so this default-run test stays green between a
// session's end and the publish bump.
//
// Run: node --test test/version-sync.test.js

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { VERSION } from "../Query.js";
import { VERSION as STREAM_VERSION } from "../StreamQuery.js";
import { VERSION as AWAIT_VERSION } from "../Awaitable.js";

const HERE = dirname(fileURLToPath(import.meta.url));
const pkg = JSON.parse(readFileSync(join(HERE, "..", "package.json"), "utf8"));

test("VERSION const equals package.json version (string compare)", () => {
    assert.equal(typeof VERSION, "string");
    assert.equal(VERSION, pkg.version);
    // The /stream and /await entries re-export the same binding, so all three
    // subpaths report one string.
    assert.equal(STREAM_VERSION, VERSION);
    assert.equal(AWAIT_VERSION, VERSION);
});
