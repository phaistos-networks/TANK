// port of https://github.com/foutaise/texttable
#pragma once
#include <switch.h>
#include <numeric>

struct text_table final {
        struct row final {
                std::vector<Buffer> columns;
        };

        enum class ColAlign : uint8_t {
                Undefined,
                Left, // columns flushed left
		Center,
                Right // columns flushed right
        } col_align{ColAlign::Left}, hdr_align{ColAlign::Center};

        enum class ColVAlign : uint8_t {
                Undefined,
                Top,
                Bottom,
                Middle
        } col_valign{ColVAlign::Middle};

        enum class DecorationFlags : uint8_t {
                Border    = 1 << 0,
                Header    = 1 << 1,
                HLines    = 1 << 2,
                VLines    = 1 << 3,
        };

        uint8_t deco{unsigned(DecorationFlags::Border)};
        std::vector<std::unique_ptr<row>> rows;
	std::vector<Buffer> header;
        size_t                            max_cols{0};
        const bool                        use_borders = false;
        // 0 for unlimited(cells won't be wrapped)
        size_t                            max_width = 0;
        std::vector<uint32_t>             columns_widths;
        char                              horizontal_char = '-';
        char                              vertical_char   = '|';
        char                              corner_char     = '+';
        char                              header_char     = '=';
        size_t row_columns{0};

	void check_row_size(const size_t n) {
		if (!row_columns)
			row_columns = n;
		else if (row_columns != n)
			throw Switch::runtime_error("Unexpected number of columns");
	}

        template <typename... T>
        void set_headers(T &&... args) {
                std::vector<Buffer> v{Buffer::build(std::forward<T>(args))...};

                header = std::move(v);
		check_row_size(header.size());
        }

        template <typename... T>
        void add_row(T &&... args) {
                auto                r = std::make_unique<row>();
                std::vector<Buffer> v{Buffer::build(std::forward<T>(args))...};

		check_row_size(v.size());
		max_cols = std::max(max_cols, v.size());
                r->columns = std::move(v);

                rows.emplace_back(std::move(r));
        }

        static size_t ansi_seq_len(const char *p, const char *const e) noexcept {
                if (p + 3 < e && *p == '\033' && p[1] == '[' && (p[2] >= '0' && p[2] <= '9')) {
                        const auto b = p;

                        for (p += 3; p < e && (*p >= '0' && *p <= '9'); ++p)
                                continue;

                        if (p < e) {
                                if (*p == 'm') {
                                        return std::distance(b, p) + 1;
                                }
                                // TODO: support more escape sequences
                        }
                }

                return 0;
        }

        static uint32_t line_length(const str_view32 s) {
		static constexpr size_t tab_width = 4;
                uint32_t    length{0}, ckpt{0};

                for (const char *p = s.data(), *const e = p + s.size(); p < e;) {
                        if (*p == '\t') {
				// https://stackoverflow.com/questions/13094690/how-many-spaces-for-tab-character-t
                                const auto span    = length - ckpt;
                                const auto aligned = (span + tab_width) & (-tab_width);

                                length += aligned - span;
                                ckpt = length;
                                ++p;
                        } else if (const auto span = ansi_seq_len(p, e)) {
                                p += span;
                        } else {
                                ++length;
                                ++p;
                        }
                }

                return length;
        }

        auto cell_length(const str_view32 bs) const noexcept {
                uint32_t max{0};

                for (const auto s : bs.split('\n')) {
			max = std::max(max, line_length(s));
                }

                return max;
        }

        auto cell_length(const Buffer &b) const noexcept {
		return cell_length(b.as_s32());
        }

        void compute_cols_width() {
                columns_widths.resize(max_cols);

                for (const auto &it : rows) {
                        const auto &cols{it->columns};
                        const auto  cnt{cols.size()};

                        for (size_t i{0}; i < cnt; ++i) {
                                const auto &b = cols[i];
                                const auto  l = cell_length(b);

                                columns_widths[i] = std::max(columns_widths[i], l);
                        }
                }

		for (size_t i{0}; i < header.size(); ++i) {
			const auto l = cell_length(header[i]);

			columns_widths[i] = std::max(columns_widths[i], l);
		}

                const auto ncols         = columns_widths.size();
                const auto content_width = std::accumulate(columns_widths.begin(), columns_widths.end(), 0);
                const auto deco_width    = 3 * (ncols - 1) + ((deco & unsigned(DecorationFlags::Border)) ? 4 : 0);

                if (max_width && (content_width + deco_width > max_width)) {
                        // content too wide to fit the expected max_width
                        // recompute max. cell width for each cell
                        const auto            normalized_max_width = std::max(ncols + deco_width, max_width);
                        auto                  available_width      = normalized_max_width - deco_width;
                        size_t                i                    = 0;
                        std::vector<uint32_t> new_res(max_cols);

                        while (available_width) {
                                if (new_res[i] < columns_widths[i]) {
                                        ++new_res[i];
                                        --available_width;
                                }
                                i = (i + 1) % ncols;
                        }

                        columns_widths = std::move(new_res);
                }

        }

        void build_hline_repr(Buffer *out, const bool header = false) {
                const auto horiz = header ? header_char : horizontal_char;

                if (corner_char)
                        out->append(corner_char);
                for (const auto width : columns_widths) {
                        memset(out->RoomFor(width), horiz, width);
                        if (corner_char)
                                out->append(corner_char);
                }
                out->pop_back();
                if (corner_char)
                        out->append(corner_char);
                out->append('\n');
        }

        struct formatted_cell final {
		uint32_t pad_top, pad_bottom;
		std::vector<std::pair<str_view32, size_t>> content;
	};

	// returns (bytes consumed, repr length)
        std::pair<size_t, size_t> truncate(str_view32 s, const size_t max) {
                const auto *p = s.data(), *const e = p + s.size(), *const b = p;
                const char *last_ws{nullptr};
                size_t      last_ws_rem, rem = max;

                while (rem && p < e) {
                        if (const auto n = ansi_seq_len(p, e)) {
                                p += n;
                        } else if (*p == '\t') {
                                // TODO:
                                last_ws     = p;
                                last_ws_rem = rem;
                                ++p;
                                --rem;
                        } else if (*p == ' ') {
                                last_ws     = p;
                                last_ws_rem = rem;
                                ++p;
                                --rem;
                        } else {
                                --rem;
                                if (!rem) {
                                        if (last_ws) {
                                                // Can safely break here
                                                // TODO: if (nullptr == last_ws) return p and an extra boolean
                                                // that indicates thar we need to append a '-' when rendering this line
                                                // and account for '-' in our calcs.
                                                p   = last_ws;
                                                rem = last_ws_rem;
                                                break;
                                        }
                                }
                                ++p;
                        }
                }

                return {std::distance(b, p), max - rem};
        }

        // split each cell to fit in the column width
        // each cell is turned into a list, result of the wrapping of the string to the desired width
        auto split_cells(const std::vector<Buffer> &row, const bool for_header, std::vector<std::unique_ptr<formatted_cell>> *const res) {
                size_t                                       max_cell_lines{0};

		res->clear();
                for (size_t i{0}; i < columns_widths.size(); ++i) {
                        const auto width = columns_widths[i];
                        const auto s     = row[i].as_s32();
                        auto       fc    = std::make_unique<formatted_cell>();

                        for (auto it : s.split('\n')) {
				auto text = it.ws_trimmed();

                                if (!text) {
                                        fc->content.emplace_back(""_s32, 0);
				} else if (!max_width) {
					fc->content.emplace_back(text, line_length(text));
				} else {
                                        do {
                                                auto [consumed, repr_length] = truncate(text, width);
                                                auto segment                 = text.Prefix(consumed);

                                                while (segment) {
                                                        if (isspace(segment.front())) {
                                                                --repr_length;
                                                                segment.strip_prefix(1);
                                                        } else if (isspace(segment.back())) {
                                                                --repr_length;
                                                                segment.strip_suffix(1);
                                                        } else
                                                                break;
                                                }

						// SLog("Segment [", segment, "] ", consumed, " ", repr_length, "\n");

                                                fc->content.emplace_back(segment, repr_length);
                                                text.strip_prefix(consumed).ws_trimmed();
                                        } while (text);
                                }
                        }

                        max_cell_lines = std::max(max_cell_lines, fc->content.size());
                        res->emplace_back(std::move(fc));
                }

                for (auto &it : *res) {
                        auto       fc         = it.get();
                        const auto cell_lines = fc->content.size();

                        if (for_header)
                                fc->pad_bottom = max_cell_lines - cell_lines;
                        else {
                                switch (col_valign) {
                                        case ColVAlign::Middle: {
                                                const auto missing = max_cell_lines - cell_lines;

                                                fc->pad_top    = missing / 2;
                                                fc->pad_bottom = max_cell_lines - cell_lines - fc->pad_top;
                                        } break;

                                        case ColVAlign::Bottom:
                                                fc->pad_top = max_cell_lines - cell_lines;
                                                break;

                                        case ColVAlign::Top:
                                                fc->pad_bottom = max_cell_lines - cell_lines;
                                                break;

                                        default:
                                                break;
                                }
                        }
                }

		return max_cell_lines;
        }

        void draw_line(Buffer *out, const std::vector<Buffer> &cols, const bool for_header, std::vector<std::unique_ptr<formatted_cell>> *const draw_line_res) {
                const auto  cnt            = cols.size();
                const auto  max_cell_lines = split_cells(cols, for_header, draw_line_res);
                const auto &c              = *draw_line_res;
                const auto  cl_content = [&](const uint32_t cell_index, const auto cell_line) -> std::pair<str_view32, size_t> {
                        const auto fc = c[cell_index].get();
			
                        if (cell_line < fc->pad_top)
                                return {""_s32, 0};
                        else if (cell_line >= fc->pad_top + fc->content.size())
                                return {""_s32, 0};
                        else
                                return fc->content[cell_line - fc->pad_top];
                };

                // one line at a time, for all cells
		// TODO: if a cell that includes ansifmt formatting is truncated
		// because of max width etc, then all other cell lines in all previous columns will also be affected
		// until we render the last cell line that likely includes an ansifmt::reset
		// there is not much we can do, other than perhaps remember all ansifmt sequenes active/cell line
		// and reset/restore them as we render cell lines.
		// For now, this is not worth the effort.
		for (size_t cell_line{0}; cell_line < max_cell_lines; ++cell_line) {
                        for (size_t cell_index{0}; cell_index < cnt; ++cell_index) {
                                const auto [line_content, line_content_len] = cl_content(cell_index, cell_line);
                                const auto col_width                        = columns_widths[cell_index];

                                out->append(vertical_char);
                                switch (for_header ? hdr_align : col_align) {
                                        case ColAlign::Undefined:
                                                [[fallthrough]];
                                        case ColAlign::Left:
                                                out->append(line_content);
                                                if (const auto n = col_width - line_content_len)
                                                        memset(out->RoomFor(n), ' ', n);
                                                break;

                                        case ColAlign::Center: {
						size_t m;

                                                if (line_content_len < col_width) {
                                                        const auto fill = col_width - line_content_len;
                                                        m               = fill / 2;
                                                        memset(out->RoomFor(m), ' ', m);
                                                } else
                                                        m = 0;

                                                out->append(line_content);

                                                if (const auto sum = m + line_content_len; sum < col_width) {
                                                        const auto rem = col_width - sum;

                                                        memset(out->RoomFor(rem), ' ', rem);
                                                }
                                        } break;

                                        case ColAlign::Right:
                                                if (const auto n = col_width - line_content_len)
                                                        memset(out->RoomFor(n), ' ', n);
                                                out->append(line_content);
                                                break;
                                }
                        }
			out->append(vertical_char);
			out->append('\n');
		}
		build_hline_repr(out, for_header);
        }

        Buffer draw() {
                // we need to determine the maximum length for each column
                Buffer b;

                if (rows.empty())
                        return b;

		std::vector<std::unique_ptr<formatted_cell>> dl_res;

                compute_cols_width();

                if (deco & unsigned(DecorationFlags::Border)) {
                        build_hline_repr(&b);
		}

		if (!header.empty()) {
			draw_line(&b, header, true, &dl_res);
		}

		for (auto &&row : rows) 
			draw_line(&b, row->columns, false, &dl_res);

                return b;
        }
};
