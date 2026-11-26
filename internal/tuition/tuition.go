// Package tuition chứa quy tắc tính học phí dùng chung (số liệu giả lập).
//
// Finance dùng để lập khoản học phí; Academic dùng để hiển thị giá trên cổng đăng ký.
// Hai bên tự tính, không gọi nhau – chỉ dùng chung quy tắc.
package tuition

// PerCredit là đơn giá một tín chỉ (VND).
const PerCredit = 450_000

// Fee trả về học phí của một học phần có số tín chỉ cho trước.
func Fee(credits int) int { return credits * PerCredit }
