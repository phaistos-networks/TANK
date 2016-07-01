// http://en.wikipedia.org/wiki/ANSI_escape_code
#pragma once

namespace ansifmt
{
	// http://en.wikipedia.org/wiki/ANSI_escape_code
	static constexpr const char *bold = "\033[1m";
	static constexpr const char *reset = "\033[0m";
	static constexpr const char *inverse = "\033[3m";

	static constexpr const char *color_black = "\033[30m";
	static constexpr const char *color_red = "\033[31m";
	static constexpr const char *color_green = "\033[32m";
	static constexpr const char *color_brown = "\033[33m";
	static constexpr const char *color_blue = "\033[34m";
	static constexpr const char *color_magenta = "\033[35m";
	static constexpr const char *color_cyan = "\033[36m";
	static constexpr const char *color_gray = "\033[37m";

	
	static constexpr const char *bgcolor_black = "\033[40m";
	static constexpr const char *bgcolor_red = "\033[41m";
	static constexpr const char *bgcolor_green = "\033[42m";
	static constexpr const char *bgcolor_brown = "\033[43m";
	static constexpr const char *bgcolor_blue = "\033[44m";
	static constexpr const char *bgcolor_magenta = "\033[45m";
	static constexpr const char *bgcolor_cyan = "\033[46m";
	static constexpr const char *bgcolor_gray = "\033[47m";

	static constexpr const char *cls = "\033[2J\033[1;1H";
};
