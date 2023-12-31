#!/usr/bin/env perl
while (<>) {
    s{(\s*<head[^>]*>.*(</head>\s*|$)|[^>]*</head>)}//;
    print unless /^\s+$/;
}
