// Nhà Thờ Online — BẢN 3D (độc lập hoàn toàn với bản 2D)
// Máy chủ Node.js + WebSocket: phục vụ trang web tĩnh trong thư mục public/
// và giữ kết nối để nhiều người cùng hiện diện trong một nhà thờ 3D.
//
// Bản này chỉ có MỘT nhà thờ: "duc-ba" (Nhà thờ Đức Bà Sài Gòn).
// Giao thức vẫn giữ field church để sau này mở rộng thêm nhà thờ khác.
//
// Không gian 3D là MỘT không gian liên tục (sân trước -> cửa chính -> lòng
// nhà thờ), không chia cảnh như bản 2D. Tọa độ di chuyển tính bằng MÉT trên
// mặt sàn: x ngang (trái/phải), z dọc trục nhà thờ (bàn thờ ở z âm,
// sân trước ở z dương).
//
// Giao thức (JSON qua WebSocket):
//   client -> server:
//     {t:"join",   name, look}          vào nhà thờ (xuất hiện ở sân trước)
//     {t:"move",   x, z, sit, kneel, zone, y}    di chuyển (mét), sit = đang ngồi ghế,
//                                       kneel = đang quỳ ở bục quỳ (loại trừ lẫn nhau với sit)
//                                       (v35) zone = khu đang đứng: "yard" (sân
//                                       ngoài) / "nave" (lòng nhà thờ, gồm cả
//                                       gác đàn) / "crypt" (tầng hầm tưởng nhớ);
//                                       y = cao độ sàn (0 ở tầng trệt, ~5,2 trên
//                                       gác đàn; hầm dùng tọa độ LỤC BỘ riêng
//                                       theo zone nên y vẫn 0). Zone lạ/không
//                                       hợp lệ -> giữ zone hiện tại; y kẹp 0..6.
//     {t:"look",   look}                đổi ngoại hình
//     {t:"candle", intention, st}       thắp nến chung (lưu bền, tối đa 200);
//                                       st = điểm thắp nến (pieta/mary/
//                                       altar-left/altar-right gần bàn thờ;
//                                       v35 cộng "loft" trên gác đàn và
//                                       "crypt" trong tầng hầm), lạ ->
//                                       "altar-left".
//                                       v8: client gửi intention TRỐNG (luồng
//                                       cầm nến không còn thẻ ý nguyện) —
//                                       server điền mặc định "Một ngọn nến
//                                       cầu nguyện" để nến vẫn có chữ.
//     {t:"carry", on}                   (v12) báo trạng thái ĐANG CẦM NẾN
//                                       trên tay: on=true khi nhân vật lấy
//                                       nến từ giỏ lên tay, on=false khi ấn
//                                       nút Hủy. Server chỉ lưu cờ + chuyển
//                                       tiếp, không kiểm tra gì thêm.
//     {t:"sign"}                        làm dấu Thánh Giá (giới hạn 1 lần / 3 giây)
//     {t:"prayer", text}                (v11) dâng lời cầu nguyện CHUNG: dòng
//                                       chữ bay lên + vệt sáng tại vị trí
//                                       người dâng, cả phòng cùng thấy.
//                                       Server cắt tối đa 280 ký tự, chuỗi
//                                       rỗng thì bỏ qua, chống spam 8 giây /
//                                       người, rồi broadcast kèm id + tên.
//     {t:"emote", kind, to}             (v28) tương tác với một người:
//                                       kind "wave" (vẫy chào) / "pray"
//                                       (cầu nguyện cho), to = id người
//                                       nhận; chống spam 2,5 giây/người.
//     {t:"chat", text}                  (v28) trò chuyện trong phòng, tối
//                                       đa 200 ký tự, chống spam 1,2 giây.
//   server -> client:
//     {t:"welcome", id, church, players, candles}   (player có thêm field
//                                       "carry" từ v12: đang cầm nến hay không)
//     {t:"join", player} / {t:"leave", id} / {t:"move", id, x, z, sit, kneel} / {t:"look", id, look}
//     {t:"carry", id, on}               (v12) một người vừa cầm nến lên tay
//                                       (on=true) hoặc vừa ấn Hủy bỏ nến
//                                       (on=false) — mọi người còn lại thấy
//                                       nến trên tay họ theo thời gian thực
//     {t:"candle", candle}
//     {t:"sign", id}
//     {t:"prayer", id, name, text, zone}      (v11) lời cầu nguyện vừa dâng — gửi
//                                       cho CẢ phòng, kể cả người dâng
//                                       (client tự chặn echo của mình theo id)
//                                       (v35) kèm zone của NGƯỜI DÂNG (lấy từ
//                                       trạng thái server, không tin client
//                                       gửi): lời dâng trong hầm (zone
//                                       "crypt") được client ghim thêm lên
//                                       tường ý nguyện của tầng hầm.
//     {t:"emote", id, name, kind, to}   (v28) một người vừa chào / cầu
//                                       nguyện cho người khác — gửi cho
//                                       mọi người TRỪ người gửi (họ đã
//                                       tự chạy hiệu ứng tại máy mình)
//     {t:"chat", id, name, text}        (v28) tin trò chuyện — gửi cho CẢ
//                                       phòng, kể cả người gửi (echo)
//     {t:"rooms", counts:{"duc-ba": soNguoi}}
//
// Tòa giải tội / góc thả nỗi lòng KHÔNG đi qua máy chủ: nội dung chỉ nằm
// trên trình duyệt của người viết rồi tan đi, server không bao giờ nhận được.

const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const PUBLIC_DIR = path.join(__dirname, "public");
const DATA_DIR = path.join(__dirname, "data");
const CANDLE_FILE = path.join(DATA_DIR, "candles.json");
const THREE_DIR = path.join(__dirname, "node_modules", "three", "build");
const THREE_ADDONS_DIR = path.join(__dirname, "node_modules", "three", "examples", "jsm");

const CHURCH = "duc-ba";
const MAX_CANDLES = 200; // quá số này, nến cũ nhất tắt dần
const MAX_NAME = 48; // (v25) nới từ 24 -> 48: tên hiển thị nay gồm TÊN THÁNH + tên thường (ví dụ "Phêrô Minh Trí") gửi gộp trong field name
const MAX_INTENTION = 140;
const MAX_PRAYER = 280;      // (v11) độ dài tối đa của lời cầu nguyện chung
const MAX_CHAT = 200;        // (v28) độ dài tối đa của một tin trò chuyện trong phòng
const PRAYER_SPAM_MS = 8000; // (v11) mỗi người tối đa 1 lượt dâng / 8 giây
// Giới hạn thế giới (mét): nhà thờ + sân
const BOUND_X = 21; // (v41) nới nhẹ cho sân dưới + cổng (kẹp di chuyển thật ở client)
const BOUND_Z_MIN = -33; // sau bàn thờ một chút
const BOUND_Z_MAX = 64.5; // (v41) bố cục mới: sân trên -> thang lớn -> sân dưới (tới ~56) -> cổng -> đoạn đường ngoài tới ~63,5
const SPAWN = { x: 0, z: -7 }; // (v40) spawn GIỮA lòng nhà thờ trên thảm đỏ, hướng bàn thờ

// ---------- tiện ích ----------
function cleanText(s, max) {
  if (typeof s !== "string") return "";
  return s.replace(/[<>&"']/g, "").trim().slice(0, max);
}
/* (v42 — điểm 15 ĐÍNH CHÍNH + 16) tùy chỉnh nhân vật RÚT GỌN: chỉ
   còn GIỚI TÍNH + MÀU TRANG PHỤC có hiệu lực (+ KHĂN VOAN cho nữ).
   Mọi field cũ trong profile đã lưu (outfit/hair/skin/hairColor/
   glasses) bị BỎ QUA an toàn và ép về chuẩn mặc định theo giới tính
   để mọi client dựng cùng một nhân vật: nam "so-mi"/tóc "ngan", nữ
   "vay"/tóc "dai", da "trung-binh", không kính. (v46: nữ đổi áo dài
   -> váy + cardigan theo ảnh mẫu anime; nam tóc nâu hạt dẻ.) */
function cleanLook(look) {
  const g = look && look.gender === "nu" ? "nu" : "nam";
  const color = ["trang", "xanh", "hong", "kem", "nau", "do", "tim", "vang"].includes(look && look.color) ? look.color : "xanh";
  const veil = g === "nu" ? !!(look && look.veil) : false; // khăn voan lễ — chỉ áp dụng cho nhân vật nữ
  return {
    gender: g,
    outfit: g === "nu" ? "vay" : "so-mi", // (v46) nữ: váy + cardigan theo ảnh mẫu anime (thay áo dài); nam: sơ mi + quần tây
    color,
    hair: g === "nu" ? "bui" : "ngan", // (v48) nữ tóc búi
    veil,
    skin: "trung-binh",
    hairColor: g === "nu" ? "den" : "vang-nau", // (v46) nam tóc nâu ấm như ảnh mẫu
    glasses: false,
  };
}
function clamp(v, min, max, fallback) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

// ---------- trạng thái ----------
const players = new Map(); // id -> player
/* Các điểm thắp nến trong nhà thờ (khớp CANDLE_POINTS phía client): mỗi ngọn
   nến mang field "st" = id điểm nó được thắp tại. Tin candle gửi st lạ/không
   có -> ép về "altar-left". Nến cũ trong file lưu (chưa có st, hoặc mang st
   "main" của điểm giữa nhà thờ đã gỡ ở v17) cũng được gán "altar-left" lúc
   nạp qua cùng hàm này. */
const CANDLE_POINT_IDS = ["mary", "mary2", "altar", "altar2", "crypt"]; // (v94) them hai diem nen giua vom moi (moi tuong hong 2 ban) // (v79) bỏ điểm nến tầng trên "loft" theo yêu cầu của Trí — nến cũ st "loft" tự ép về "altar" qua cleanCandleSt // (v67) gộp hai điểm bàn thờ thành "altar" // (v40) ĐÃ GỠ điểm "pieta" — nến cũ mang st "pieta" tự ép về "altar-left" qua cleanCandleSt
function cleanCandleSt(st) { return CANDLE_POINT_IDS.includes(st) ? st : "altar"; } // (v67) nến cũ altar-left/right/pieta ép về điểm gộp "altar"
/* (v35) KHU VỰC (zone) của người chơi: "yard" sân ngoài / "nave" lòng nhà
   thờ (gác đàn vẫn thuộc nave — cao độ phân biệt bằng field y) / "crypt"
   tầng hầm tưởng nhớ (tọa độ x/z là tọa độ CỤC BỘ của phòng hầm). Mọi giá
   trị lạ đều chuẩn hóa về "nave" để client cũ/kỳ lạ không phá phòng. */
const ZONES = ["yard", "nave", "crypt"];
function cleanZone(z, fallback) { return ZONES.includes(z) ? z : (fallback || "nave"); }
let candles = [];
try {
  const saved = JSON.parse(fs.readFileSync(CANDLE_FILE, "utf8"));
  if (Array.isArray(saved[CHURCH])) candles = saved[CHURCH].slice(-MAX_CANDLES);
  for (const c of candles) c.st = cleanCandleSt(c.st);
  console.log("Đã nạp nến từ", CANDLE_FILE);
} catch {
  console.log("Chưa có file nến — bắt đầu trống.");
}
let saveTimer = null;
function saveCandles() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFile(CANDLE_FILE, JSON.stringify({ [CHURCH]: candles }), () => {});
  }, 500);
}

// ---------- khóa quản trị của chủ máy chủ (v28/v30) ----------
// Lấy từ biến môi trường DONATE_ADMIN_KEY hoặc file data/admin-key.txt
// do CHỦ MÁY CHỦ tự tạo trên máy chủ — file không nằm trong gói cài
// đặt, người chơi không có và không thấy.
function readAdminKey(){
  if (process.env.DONATE_ADMIN_KEY) return String(process.env.DONATE_ADMIN_KEY).trim();
  try { return fs.readFileSync(path.join(DATA_DIR, "admin-key.txt"), "utf8").trim(); } catch { return ""; }
}

// ---------- (v65) TRẠNG THÁI GIÁNG SINH TOÀN MÁY CHỦ ----------
// Chủ máy chủ bật/tắt tại trang /admin (kèm khóa quản trị); trạng
// thái lưu ở data/xmas.json để giữ qua lần khởi động lại máy chủ,
// gửi kèm tin "welcome" cho người mới vào và phát ngay {t:"xmas"}
// cho mọi người chơi đang nối. Trong game KHÔNG còn nút Giáng Sinh.
const XMAS_FILE = path.join(DATA_DIR, "xmas.json");
let xmasOnServer = (() => { try { return JSON.parse(fs.readFileSync(XMAS_FILE, "utf8")).on === true; } catch { return false; } })();
function saveXmasState(){
  try { fs.mkdirSync(DATA_DIR, { recursive: true }); fs.writeFileSync(XMAS_FILE, JSON.stringify({ on: xmasOnServer })); } catch {}
}

// Trang /admin: biểu mẫu thiết lập mã QR + thông tin ngân hàng cho
// chủ máy chủ. Trang tự chứa (không file ngoài), phần script chỉ dùng
// nối chuỗi thường để tránh xung đột cú pháp với template bao ngoài.
const ADMIN_PAGE_HTML = `<!DOCTYPE html>
<html lang="vi"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Thiết lập chủ server — Thánh Đường</title>
<style>
  body{font-family:"Segoe UI",system-ui,sans-serif;background:#171310;color:#f3e9d2;margin:0;padding:22px 14px 40px;}
  .wrap{max-width:1020px;margin:0 auto;}
  .grid{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;align-items:stretch;}
  .grid .card{margin-bottom:0;}
  .grid .wide{grid-column:1 / -1;}
  .tile{cursor:pointer;}
  .tile:hover{border-color:rgba(255,217,104,.8);}
  .tile .d{color:#a6967a;font-size:13px;line-height:1.5;}
  .tile .s{margin-top:8px;font-size:13px;color:#ffd968;font-weight:600;}
  #popOverlay{display:none;position:fixed;inset:0;background:rgba(8,6,4,.74);z-index:60;overflow-y:auto;padding:18px 10px 34px;box-sizing:border-box;}
  .popBox{max-width:620px;margin:0 auto;background:#241c14;border:1px solid rgba(231,207,154,.45);border-radius:16px;padding:16px;position:relative;box-sizing:border-box;}
  .popBox .card{margin-bottom:0;}
  #popClose{position:absolute;top:-14px;right:-10px;width:38px;height:38px;margin:0;padding:0;border-radius:50%;font-size:16px;font-weight:700;line-height:1;z-index:6;background:rgba(22,15,7,.78);color:#ffe9a8;border:1px solid rgba(231,207,154,.85);box-shadow:0 2px 10px rgba(0,0,0,.5);display:flex;align-items:center;justify-content:center;cursor:pointer;}
  .switch{position:relative;width:52px;height:28px;border-radius:999px;border:1px solid rgba(231,207,154,.55);background:rgba(231,207,154,.15);padding:0;cursor:pointer;margin-top:8px;}
  .switch .knob{position:absolute;top:2px;left:2px;width:22px;height:22px;border-radius:50%;background:#d8c49a;transition:left .18s,background .18s;}
  .switch.on{background:#2f7d46;border-color:#57b06e;}
  .switch.on .knob{left:26px;background:#fff;}
  .keyrow{border:1px dashed rgba(231,207,154,.35);border-radius:12px;padding:10px 12px;margin:0 0 12px;}
  .keyrow label{margin-top:0;}
  @media (max-width:720px){ .grid{grid-template-columns:1fr 1fr;} .stats{grid-template-columns:1fr 1fr;} }
  select{width:100%;box-sizing:border-box;border:1px solid rgba(231,207,154,.35);border-radius:10px;padding:10px 12px;font-size:15px;font-family:inherit;background:rgba(255,250,235,.07);color:#f3e9d2;}
  select option{color:#171310;}
  .rowflex{display:flex;gap:8px;align-items:stretch;}
  .rowflex > input{flex:1;}
  .rowflex button{width:auto;margin-top:0;flex:0 0 auto;}
  .mu-alb{font-size:13px;font-weight:700;color:#ffd968;margin:12px 0 2px;}
  h1{font-size:20px;margin:0 0 4px;color:#ffe9a8;}
  .sub{color:#a6967a;font-size:13.5px;line-height:1.55;margin:0 0 18px;}
  .card{background:rgba(255,250,235,.05);border:1px solid rgba(231,207,154,.25);border-radius:14px;padding:16px;margin-bottom:14px;}
  .card h2{font-size:15.5px;margin:0 0 10px;color:#ffd968;}
  label{display:block;font-size:13px;color:#a6967a;margin:10px 0 5px;}
  input{width:100%;box-sizing:border-box;border:1px solid rgba(231,207,154,.35);border-radius:10px;padding:10px 12px;font-size:15px;font-family:inherit;background:rgba(255,250,235,.07);color:#f3e9d2;}
  button{background:linear-gradient(180deg,#c49a3f,#a57c26);color:#fff;border:none;border-radius:11px;padding:11px 18px;font-size:15px;font-weight:700;cursor:pointer;margin-top:12px;width:100%;}
  #qrPrev{width:220px;height:220px;object-fit:contain;background:#fff;border-radius:12px;padding:8px;display:block;}
  #st{position:sticky;top:10px;z-index:5;background:rgba(62,46,14,.95);border:1px solid rgba(255,217,104,.55);border-radius:10px;padding:10px 58px 10px 12px;margin:0 0 14px;font-size:14px;color:#ffd968;min-height:20px;line-height:1.5;}
  .stats{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;}
  .stat{background:rgba(255,250,235,.05);border:1px solid rgba(231,207,154,.18);border-radius:10px;padding:10px 12px;}
  .stat .v{font-size:17px;font-weight:700;color:#ffe9a8;}
  .stat .l{font-size:12px;color:#a6967a;margin-top:2px;}
</style></head><body><div class="wrap">
<h1>✝ Thiết lập chủ server</h1>
<p class="sub">Trang này chỉ dành cho chủ server của Thánh Đường: thiết lập MÃ QR quyên góp, thông tin ngân hàng hiện trong nhà thờ, bật/tắt chế độ Giáng Sinh và quản lý danh sách nhạc thánh ca cho toàn nhà thờ. Mọi thay đổi phải kèm khóa quản trị mới lưu được. Bấm vào một thẻ bên dưới là mở phần thiết lập riêng của thẻ đó. Lần đầu chưa có khóa thì bấm thẻ Khóa quản trị để tự đặt mã.</p>
<div class="card">
  <h2>Tổng quan</h2>
  <div class="stats">
    <div class="stat"><div class="v" id="ovPlayers">…</div><div class="l">người đang trong nhà thờ</div></div>
    <div class="stat"><div class="v" id="ovXmas">…</div><div class="l">chế độ Giáng Sinh</div></div>
    <div class="stat"><div class="v" id="ovMusic">…</div><div class="l">bài nhạc thánh ca</div></div>
    <div class="stat"><div class="v" id="ovQr">…</div><div class="l">mã QR quyên góp</div></div>
  </div>
</div>
<div class="grid">
<div class="card tile" id="tileKey"><h2>🔑 Khóa quản trị</h2><div class="d">Đặt mã quản trị lần đầu hoặc đổi mã (đổi mã cần nhập mã cũ).</div></div>
<div class="card tile" id="tileXmas"><h2>🎄 Giáng Sinh</h2><div class="d">Bật / tắt chế độ Giáng Sinh cho toàn nhà thờ.</div><div class="s" id="tileXmasState">…</div><button id="tileXmasToggle" class="switch" role="switch" aria-checked="false" title="Bật / tắt Giáng Sinh"><span class="knob"></span></button></div>
<div class="card tile" id="tileMusic"><h2>🎵 Nhạc thánh ca</h2><div class="d">Thêm, xóa và chia album cho danh sách nhạc phát trong nhà thờ.</div><div class="s" id="tileMusicState">…</div></div>
<div class="card tile" id="tileQr"><h2>Mã QR quyên góp</h2><div class="d">Đổi ảnh mã QR hiện trong mục đóng góp của nhà thờ.</div><div class="s" id="tileQrState">…</div></div>
<div class="card tile" id="tileBank"><h2>Ngân hàng</h2><div class="d">Thông tin ngân hàng hiện dưới mã QR quyên góp.</div></div>
</div>
</div>
<div id="popOverlay"><div class="popBox">
<button id="popClose" title="Đóng">✕</button>
<div id="st">Sẵn sàng.</div>
<div class="keyrow"><label for="k">Khóa quản trị (nhập một lần, dùng cho mọi mục)</label><input type="password" id="k" autocomplete="off" placeholder="Khóa quản trị"></div>
<div class="card" id="setupCard" style="display:none;border-color:#ffd968;">
  <h2>Đặt khóa quản trị (lần đầu)</h2>
  <p style="color:#a6967a;font-size:13.5px;line-height:1.55;margin:0;">Máy chủ chưa có khóa quản trị nên hiện chưa lưu được gì. Hãy tự đặt một khóa bạn dễ nhớ (ít nhất 4 ký tự) — từ nay mỗi lần sửa ở trang này bạn nhập khóa đó. Chỉ đặt được MỘT lần lúc chưa có khóa.</p>
  <label for="kNew">Khóa mới của bạn</label>
  <input type="password" id="kNew" autocomplete="off" placeholder="Ví dụ: matkhau123">
  <button id="btnSetup">Đặt khóa quản trị</button>
</div>
<div class="card" id="cardKey" style="display:none;">
  <h2>Khóa quản trị</h2>
  <p style="color:#a6967a;font-size:13.5px;line-height:1.55;margin:0;">Khóa quản trị nằm ở ô ngay phía trên trong popup này — nhập một lần là dùng cho mọi mục (mã QR, ngân hàng, Giáng Sinh, nhạc). Đây là khóa bạn tự đặt khi mở trang này lần đầu; hãy giữ khóa này cho riêng mình để người lạ không sửa được thiết lập của nhà thờ.</p>
  <h2 style="margin-top:18px;">Đổi mã quản trị</h2>
  <p style="color:#a6967a;font-size:13.5px;line-height:1.55;margin:0;">Muốn đổi mã thì nhập đúng MÃ CŨ và mã mới muốn dùng — đổi xong trang tự điền mã mới vào ô khóa ở trên.</p>
  <label for="kOld">Mã cũ</label>
  <input type="password" id="kOld" autocomplete="off" placeholder="Nhập mã hiện tại">
  <label for="kChange">Mã mới</label>
  <input type="password" id="kChange" autocomplete="off" placeholder="Ít nhất 4 ký tự">
  <button id="btnChangeKey">Đổi mã quản trị</button>
</div>
<div class="card" id="cardQr" style="display:none;">
  <h2>Mã QR quyên góp</h2>
  <img id="qrPrev" alt="Mã QR hiện tại" src="/donate-qr">
  <div id="qrNone" style="display:none;color:#a6967a;font-size:13.5px;">Chưa có mã QR nào.</div>
  <label for="qrFile">Chọn ảnh mã QR mới (PNG / JPG / WebP, tối đa 3MB)</label>
  <input type="file" id="qrFile" accept="image/png,image/jpeg,image/webp">
  <button id="btnQr">Lưu mã QR mới</button>
</div>
<div class="card" id="cardBank" style="display:none;">
  <h2>Thông tin ngân hàng (hiện dưới mã QR)</h2>
  <label for="bBank">Ngân hàng</label>
  <input type="text" id="bBank" placeholder="Ví dụ: Vietcombank">
  <label for="bAcc">Số tài khoản</label>
  <input type="text" id="bAcc" placeholder="Ví dụ: 0123456789">
  <label for="bHolder">Tên chủ tài khoản</label>
  <input type="text" id="bHolder" placeholder="Ví dụ: NGUYEN VAN A">
  <button id="btnInfo">Lưu thông tin ngân hàng</button>
</div>
<div class="card" id="cardXmas" style="display:none;">
  <h2>Chế độ Giáng Sinh (toàn nhà thờ)</h2>
  <p style="color:#a6967a;font-size:13.5px;line-height:1.55;margin:0;">Bật là MỌI người chơi đang ở trong nhà thờ thấy ngay trang trí Giáng Sinh (dây đèn, cây thông, trời đêm ở sân); người vào sau cũng thấy. Trạng thái được lưu trên máy chủ. Trong game không còn nút Giáng Sinh nữa — chỉ bật/tắt được tại trang này, kèm khóa quản trị ở trên.</p>
  <label>Trạng thái hiện tại</label>
  <div id="xmasState" style="font-size:15px;color:#ffd968;">Đang tải…</div>
  <button id="btnXmas">Bật Giáng Sinh</button>
</div>
<div class="card" id="cardMusic" style="display:none;">
  <h2>Nhạc thánh ca (danh sách phát trong nhà thờ)</h2>
  <p style="color:#a6967a;font-size:13.5px;line-height:1.55;margin:0 0 6px;">Thêm một hoặc nhiều file nhạc trong một lượt (.mp3, .ogg, .m4a — tối đa 100MB mỗi file) là các bài đó xuất hiện ngay trong panel Thánh Ca cho mọi người chơi; muốn bỏ bài nào thì bấm nút Xóa ở bài đó. Chỉ dùng những file nhạc bạn có quyền sử dụng.</p>
  <div id="muList" style="margin:6px 0 4px;color:#a6967a;font-size:13.5px;">Đang tải danh sách…</div>
  <label for="muAlbum">Album cho các bài sắp thêm (để trống = không chia album)</label>
  <select id="muAlbum"><option value="">— Không album (thư mục gốc) —</option></select>
  <label for="muAlbumNew">Hoặc tạo album mới</label>
  <div class="rowflex"><input type="text" id="muAlbumNew" placeholder="Ví dụ: Thánh ca Giáng Sinh"><button id="btnAlbum" type="button">Tạo album</button></div>
  <label for="muFile">Chọn một hoặc nhiều file nhạc mới (giữ Ctrl/Cmd để chọn nhiều file cùng lúc)</label>
  <input type="file" id="muFile" accept=".mp3,.ogg,.m4a,audio/*" multiple>
  <button id="btnMu">Thêm các bài nhạc đã chọn</button>
</div>
</div></div>
<script>
  function el(id){ return document.getElementById(id); }
  function say(m){ el("st").textContent = m; }
  function key(){ return el("k").value.trim(); }
  var qrPrev = el("qrPrev");
  qrPrev.onerror = function(){ qrPrev.style.display = "none"; el("qrNone").style.display = "block"; el("ovQr").textContent = "Chưa có"; el("tileQrState").textContent = "Chưa có mã QR"; };
  qrPrev.onload = function(){ qrPrev.style.display = "block"; el("qrNone").style.display = "none"; el("ovQr").textContent = "Đã có"; el("tileQrState").textContent = "Đã có mã QR"; };
  var hasKeyFlag = true; // (v91) trang chi ghi trang thai khoa — KHONG con tu mo popup luc moi vao trang theo yeu cau cua Tri
  fetch("/api/admin/status").then(function(r){ return r.ok ? r.json() : null; }).then(function(j){
    if (j) hasKeyFlag = !!j.hasKey;
  }).catch(function(){});
  el("btnSetup").onclick = function(){
    var nk = el("kNew").value.trim();
    if (nk.length < 4){ say("Khóa phải có ít nhất 4 ký tự."); return; }
    say("Đang đặt khóa…");
    fetch("/api/admin/setup", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ key: nk }) })
      .then(function(r){ return r.json(); }).then(function(j){
        if (j && j.ok){ el("k").value = nk; openPop("cardKey"); say("Đã đặt khóa quản trị xong. Từ giờ dùng khóa này để lưu mọi mục."); }
        else say("Không đặt được khóa: " + ((j && j.error) || "lỗi lạ") + ".");
      }).catch(function(){ say("Không nối được tới máy chủ."); });
  };
  el("btnChangeKey").onclick = function(){
    var ok = el("kOld").value.trim(), nk = el("kChange").value.trim();
    if (!ok){ say("Bạn chưa nhập mã cũ."); return; }
    if (nk.length < 4){ say("Mã mới phải có ít nhất 4 ký tự."); return; }
    say("Đang đổi mã…");
    fetch("/api/admin/change-key", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ key: ok, newKey: nk }) })
      .then(function(r){ return r.json(); }).then(function(j){
        if (j && j.ok){ el("k").value = nk; el("kOld").value = ""; el("kChange").value = ""; hasKeyFlag = true; say("Đã đổi mã quản trị xong. Từ giờ dùng mã mới để lưu mọi mục."); }
        else say("Không đổi được mã: " + ((j && j.error) || "lỗi lạ") + ".");
      }).catch(function(){ say("Không nối được tới máy chủ."); });
  };
  fetch("/donate-info").then(function(r){ return r.ok ? r.json() : null; }).then(function(j){
    if (!j) return;
    el("bBank").value = j.bank || ""; el("bAcc").value = j.account || ""; el("bHolder").value = j.holder || "";
  }).catch(function(){});
  el("btnQr").onclick = function(){
    if (!key()){ say("Bạn chưa nhập khóa quản trị."); return; }
    var f = el("qrFile").files && el("qrFile").files[0];
    if (!f){ say("Bạn chưa chọn ảnh mã QR."); return; }
    if (f.size > 3000000){ say("Ảnh quá lớn (tối đa 3MB)."); return; }
    say("Đang lưu mã QR…");
    var rd = new FileReader();
    rd.onload = function(){
      fetch("/api/admin/qr", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ key: key(), dataUrl: rd.result }) })
        .then(function(r){ return r.json(); })
        .then(function(j){
          if (j.ok){ say("Đã lưu mã QR mới — người chơi trong nhà thờ sẽ thấy mã mới."); qrPrev.src = "/donate-qr?v=" + Date.now(); }
          else say(j.error || "Không lưu được.");
        })
        .catch(function(){ say("Không kết nối được máy chủ."); });
    };
    rd.readAsDataURL(f);
  };
  var xmasOnAdmin = false;
  function drawXmas(){
    el("ovXmas").textContent = xmasOnAdmin ? "Đang bật 🎄" : "Đang tắt";
    el("xmasState").textContent = xmasOnAdmin ? "🎄 ĐANG BẬT" : "Đang tắt";
    el("tileXmasState").textContent = xmasOnAdmin ? "🎄 Đang bật" : "Đang tắt";
    el("btnXmas").textContent = xmasOnAdmin ? "Tắt Giáng Sinh" : "Bật Giáng Sinh";
    var sw = el("tileXmasToggle"); if (sw){ sw.classList.toggle("on", xmasOnAdmin); sw.setAttribute("aria-checked", xmasOnAdmin ? "true" : "false"); }
  }
  fetch("/xmas-state").then(function(r){ return r.json(); }).then(function(j){ xmasOnAdmin = !!(j && j.on); drawXmas(); }).catch(function(){ el("xmasState").textContent = "Không đọc được trạng thái."; });
  function toggleXmas(){
    if (!key()){ openPop(hasKeyFlag ? "cardKey" : "setupCard"); say("Nhập khóa quản trị, rồi bấm công tắc Giáng Sinh ngay trên thẻ."); return; }
    say("Đang đổi chế độ Giáng Sinh…");
    fetch("/api/admin/xmas", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ key: key(), on: !xmasOnAdmin }) })
      .then(function(r){ return r.json(); })
      .then(function(j){
        if (j.ok){ xmasOnAdmin = j.on; drawXmas(); say(xmasOnAdmin ? "Đã BẬT chế độ Giáng Sinh — mọi người chơi trong nhà thờ sẽ thấy ngay." : "Đã TẮT chế độ Giáng Sinh."); }
        else say(j.error || "Không đổi được.");
      })
      .catch(function(){ say("Không kết nối được máy chủ."); });
  }
  el("btnXmas").onclick = toggleXmas;
  el("tileXmasToggle").onclick = function(e){ e.stopPropagation(); toggleXmas(); };
  el("btnInfo").onclick = function(){
    if (!key()){ say("Bạn chưa nhập khóa quản trị."); return; }
    say("Đang lưu thông tin…");
    fetch("/api/admin/info", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ key: key(), bank: el("bBank").value, account: el("bAcc").value, holder: el("bHolder").value }) })
      .then(function(r){ return r.json(); })
      .then(function(j){ say(j.ok ? "Đã lưu thông tin ngân hàng." : (j.error || "Không lưu được.")); })
      .catch(function(){ say("Không kết nối được máy chủ."); });
  };
  function loadMusic(){
    fetch("/api/music").then(function(r){ return r.json(); }).then(function(list){
      var box = el("muList");
      box.innerHTML = "";
      if (!list || !list.length){ box.textContent = "Chưa có bài nhạc nào."; el("ovMusic").textContent = "0"; el("tileMusicState").textContent = "0 bài"; return; }
      var curAlb = null;
      list.forEach(function(t){
        if ((t.album || "") !== curAlb){
          curAlb = t.album || "";
          var hd = document.createElement("div");
          hd.className = "mu-alb";
          hd.textContent = curAlb ? ("📁 Album: " + curAlb) : "📄 Chưa chia album";
          box.appendChild(hd);
        }
        var row = document.createElement("div");
        row.style.cssText = "display:flex;align-items:center;gap:10px;padding:7px 0;border-bottom:1px solid rgba(231,207,154,.12);";
        var nm = document.createElement("div");
        nm.style.cssText = "flex:1;font-size:14px;color:#f3e9d2;word-break:break-all;";
        nm.textContent = t.title;
        var sz = document.createElement("div");
        sz.style.cssText = "font-size:12.5px;color:#a6967a;white-space:nowrap;";
        sz.textContent = t.size ? (t.size >= 1048576 ? (t.size / 1048576).toFixed(1) + " MB" : Math.max(1, Math.round(t.size / 1024)) + " KB") : "";
        var del = document.createElement("button");
        del.textContent = "Xóa";
        del.style.cssText = "width:auto;margin:0;padding:6px 12px;font-size:13px;";
        del.onclick = function(){
          if (!key()){ say("Bạn chưa nhập khóa quản trị."); return; }
          if (!confirm('Xóa bài "' + t.file + '" khỏi nhà thờ?')) return;
          fetch("/api/admin/music-delete", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ key: key(), file: t.file }) })
            .then(function(r){ return r.json(); })
            .then(function(j){ if (j.ok){ say("Đã xóa bài " + t.file + "."); loadMusic(); } else say(j.error || "Không xóa được."); })
            .catch(function(){ say("Không kết nối được máy chủ."); });
        };
        row.appendChild(nm); row.appendChild(sz); row.appendChild(del); box.appendChild(row);
      });
      el("ovMusic").textContent = String(list.length);
      el("tileMusicState").textContent = list.length + " bài";
    }).catch(function(){ el("muList").textContent = "Không đọc được danh sách nhạc."; });
  }
  function loadOverview(){
    fetch("/api/admin/status").then(function(r){ return r.ok ? r.json() : null; }).then(function(j){
      if (!j) return;
      el("ovPlayers").textContent = String(j.players || 0);
      if (typeof j.xmas === "boolean") el("ovXmas").textContent = j.xmas ? "Đang bật 🎄" : "Đang tắt";
    }).catch(function(){});
  }
  function loadAlbums(sel){
    fetch("/api/admin/status").then(function(r){ return r.ok ? r.json() : null; }).then(function(j){
      var albums = (j && j.albums) || [];
      var s = el("muAlbum");
      var cur = (typeof sel === "string") ? sel : s.value;
      s.innerHTML = "";
      var o0 = document.createElement("option"); o0.value = ""; o0.textContent = "— Không album (thư mục gốc) —"; s.appendChild(o0);
      albums.forEach(function(a){ var o = document.createElement("option"); o.value = a; o.textContent = a; s.appendChild(o); });
      s.value = cur || "";
    }).catch(function(){});
  }
  el("btnAlbum").onclick = function(){
    if (!key()){ say("Bạn chưa nhập khóa quản trị."); return; }
    var nm = el("muAlbumNew").value.trim();
    if (!nm){ say("Bạn chưa nhập tên album mới."); return; }
    say("Đang tạo album…");
    fetch("/api/admin/album", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ key: key(), name: nm }) })
      .then(function(r){ return r.json(); })
      .then(function(j){
        if (j.ok){ el("muAlbumNew").value = ""; loadAlbums(j.album); say('Đã tạo album "' + j.album + '" — các bài thêm tiếp theo sẽ vào album này.'); }
        else say(j.error || "Không tạo được album.");
      })
      .catch(function(){ say("Không kết nối được máy chủ."); });
  };
  loadOverview();
  setInterval(loadOverview, 20000);
  loadMusic();
  loadAlbums();
  el("btnMu").onclick = function(){
    if (!key()){ say("Bạn chưa nhập khóa quản trị."); return; }
    var picked = el("muFile").files;
    if (!picked || !picked.length){ say("Bạn chưa chọn file nhạc."); return; }
    var list = [];
    for (var i = 0; i < picked.length; i++) list.push(picked[i]);
    var okNames = [], badNames = [], idx = 0;
    function next(){
      if (idx >= list.length){
        var msg = "";
        if (okNames.length) msg += "Đã thêm " + okNames.length + " bài: " + okNames.join(", ") + ". ";
        if (badNames.length) msg += "Không thêm được " + badNames.length + " file: " + badNames.join(", ") + ".";
        if (!msg) msg = "Không có file nào được thêm.";
        if (okNames.length) msg += " Người chơi mở lại panel Thánh Ca sẽ thấy bài mới.";
        say(msg);
        el("muFile").value = "";
        loadMusic();
        loadAlbums();
        return;
      }
      var f = list[idx]; idx++;
      if (!/\\.(mp3|ogg|m4a)$/i.test(f.name)){ badNames.push(f.name + " (sai định dạng)"); next(); return; }
      if (f.size > 100000000){ badNames.push(f.name + " (quá 100MB)"); next(); return; }
      say("Đang tải lên bài " + idx + "/" + list.length + ": " + f.name + " …");
      var rd = new FileReader();
      rd.onload = function(){
        fetch("/api/admin/music", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ key: key(), name: f.name, album: el("muAlbum").value, dataUrl: rd.result }) })
          .then(function(r){ return r.json(); })
          .then(function(j){ if (j.ok) okNames.push(j.file); else badNames.push(f.name + " (" + (j.error || "lỗi máy chủ") + ")"); next(); })
          .catch(function(){ badNames.push(f.name + " (không kết nối được)"); next(); });
      };
      rd.onerror = function(){ badNames.push(f.name + " (không đọc được file)"); next(); };
      rd.readAsDataURL(f);
    }
    next();
  };
  var popPanels = ["setupCard", "cardKey", "cardQr", "cardBank", "cardXmas", "cardMusic"];
  function openPop(id){
    for (var i = 0; i < popPanels.length; i++) el(popPanels[i]).style.display = (popPanels[i] === id) ? "block" : "none";
    el("popOverlay").style.display = "block";
    document.body.style.overflow = "hidden";
  }
  function closePop(){ el("popOverlay").style.display = "none"; document.body.style.overflow = ""; }
  el("tileKey").onclick = function(){ openPop(hasKeyFlag ? "cardKey" : "setupCard"); }; // (v91) chua co khoa thi mo thang muc Dat khoa
  // (v92) the Giang Sinh khong mo popup nua — cong tac bat/tat ngay tren the
  el("tileMusic").onclick = function(){ openPop("cardMusic"); };
  el("tileQr").onclick = function(){ openPop("cardQr"); };
  el("tileBank").onclick = function(){ openPop("cardBank"); };
  el("popClose").onclick = closePop;
  el("popOverlay").onclick = function(e){ if (e.target === el("popOverlay")) closePop(); };
  document.addEventListener("keydown", function(e){ if (e.key === "Escape") closePop(); });
</script>
</body></html>`;

// ---------- HTTP ----------
// CẤM CACHE tuyệt đối cho mọi nội dung tĩnh: người dùng cập nhật bản mới bằng
// cách giải nén đè thư mục rồi mở lại trang — trình duyệt phải luôn lấy bản
// mới nhất từ server, không giữ bản cũ trong cache (đã từng xảy ra: giao diện
// cũ còn nguyên dù file trên đĩa đã mới). Kèm Pragma/Expires cho trình duyệt cũ.
const NO_STORE = {
  "Cache-Control": "no-store, no-cache, must-revalidate",
  "Pragma": "no-cache",
  "Expires": "0",
};
const MIME = { ".html": "text/html; charset=utf-8", ".css": "text/css", ".js": "text/javascript", ".mjs": "text/javascript", ".png": "image/png", ".jpg": "image/jpeg", ".svg": "image/svg+xml", ".ico": "image/x-icon", ".mp3": "audio/mpeg", ".ogg": "audio/ogg", ".m4a": "audio/mp4", ".txt": "text/plain; charset=utf-8", ".glb": "model/gltf-binary" }; // (v36) .glb: mô hình 3D thật trong public/models/
const MUSIC_DIR = path.join(PUBLIC_DIR, "music");
const httpServer = http.createServer((req, res) => {
  let p = decodeURIComponent((req.url || "/").split("?")[0]);

  // Three.js phục vụ ngay từ node_modules của dự án — client không cần CDN ngoài.
  // LƯU Ý: three.module.js của three đời mới tự import "./three.core.js",
  // nên phải phục vụ MỌI file .js trong thư mục build, không chỉ three.module.js.
  // Addons của three (postprocessing, controls...): phục vụ cả cây thư mục
  // examples/jsm vì các file import tương đối lẫn nhau. Chỉ cho phép đường dẫn
  // tương đối an toàn: từng đoạn chỉ gồm chữ/số/dấu . _ -, không "..", đuôi .js.
  if (p.startsWith("/vendor/addons/")) {
    const rel = p.slice("/vendor/addons/".length);
    const segs = rel.split("/");
    const safe = rel.endsWith(".js") && segs.every((s) => /^[\w][\w.\-]*$/.test(s) && s !== ".." && s !== ".");
    const file = safe ? path.join(THREE_ADDONS_DIR, rel) : null;
    if (!file || !file.startsWith(THREE_ADDONS_DIR)) { res.writeHead(404); return res.end("Not found"); }
    return fs.readFile(file, (err, data) => {
      if (err) { res.writeHead(404); return res.end("addon file not found — run: npm install"); }
      res.writeHead(200, { "Content-Type": "text/javascript; charset=utf-8", ...NO_STORE });
      res.end(data);
    });
  }

  if (p.startsWith("/vendor/")) {
    const name = p.slice("/vendor/".length);
    if (!/^[\w][\w.\-]*\.js$/.test(name) || name.includes("..")) { res.writeHead(404); return res.end("Not found"); }
    return fs.readFile(path.join(THREE_DIR, name), (err, data) => {
      if (err) { res.writeHead(404); return res.end("vendor file not found — run: npm install"); }
      res.writeHead(200, { "Content-Type": "text/javascript; charset=utf-8", ...NO_STORE });
      res.end(data);
    });
  }

  // (v30) TRANG THIẾT LẬP RIÊNG CHO CHỦ MÁY CHỦ tại /admin — ngoài
  // giao diện trò chơi (người chơi trong app không thấy trang này).
  // Xem trang thì tự do, nhưng LƯU thay đổi (mã QR / thông tin ngân
  // hàng) bắt buộc có khóa quản trị (readAdminKey ở trên).
  if (p === "/admin") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", ...NO_STORE });
    return res.end(ADMIN_PAGE_HTML);
  }

  // (v65) TRẠNG THÁI GIÁNG SINH — client đọc lúc vào game (trạng
  // thái cũng được gửi kèm tin "welcome" và phát trực tiếp khi đổi).
  if (p === "/xmas-state") {
    const JSONH = { "Content-Type": "application/json; charset=utf-8", ...NO_STORE };
    res.writeHead(200, JSONH);
    return res.end(JSON.stringify({ on: xmasOnServer }));
  }

  // (v65) BẬT/TẮT GIÁNG SINH — chỉ chủ máy chủ, từ trang /admin kèm
  // khóa quản trị: lưu trạng thái rồi phát cho mọi người chơi.
  // (v82) TRẠNG THÁI KHÓA QUẢN TRỊ cho trang /admin: chỉ cho biết
  // ĐÃ có khóa hay chưa (không lộ khóa) để trang hiện mục đặt khóa
  // lần đầu khi cần.
  if (p === "/api/admin/status") {
    const JSONH = { "Content-Type": "application/json; charset=utf-8", ...NO_STORE };
    res.writeHead(200, JSONH);
    let albums = [];
    try { albums = fs.readdirSync(MUSIC_DIR, { withFileTypes: true }).filter((d) => d.isDirectory()).map((d) => d.name).sort(); } catch (e) {}
    return res.end(JSON.stringify({ ok: true, hasKey: !!readAdminKey(), players: players.size, xmas: xmasOnServer, albums: albums })); // (v87) albums: cac thu muc con cua public/music/ cho o chon album o trang admin
  }

  // (v82) ĐẶT KHÓA QUẢN TRỊ LẦN ĐẦU — sửa lỗi trang admin trên máy
  // người dùng: bản đóng gói không kèm thư mục data/ nên không có
  // sẵn data/admin-key.txt, mọi nút Lưu đều bị 403 mà người dùng
  // không có cách tạo file. Nay đặt khóa ngay trên trang /admin;
  // chỉ chạy khi CHƯA có khóa nào, đã có khóa thì khóa cứng.
  if (p === "/api/admin/setup") {
    const JSONH = { "Content-Type": "application/json; charset=utf-8", ...NO_STORE };
    if (req.method !== "POST") { res.writeHead(405, JSONH); return res.end(JSON.stringify({ ok: false, error: "Chỉ dùng POST." })); }
    if (readAdminKey()) { res.writeHead(403, JSONH); return res.end(JSON.stringify({ ok: false, error: "Máy chủ đã có khóa quản trị rồi." })); }
    let body = "";
    req.on("data", (ch) => { body += ch; if (body.length > 4000) req.destroy(); });
    req.on("end", () => {
      let j;
      try { j = JSON.parse(body); } catch { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "Dữ liệu không hợp lệ." })); }
      const nk = j && typeof j.key === "string" ? j.key.trim() : "";
      if (nk.length < 4 || nk.length > 100) { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "Khóa phải có từ 4 tới 100 ký tự." })); }
      try {
        fs.mkdirSync(DATA_DIR, { recursive: true });
        fs.writeFileSync(path.join(DATA_DIR, "admin-key.txt"), nk + "\n", "utf8");
      } catch { res.writeHead(500, JSONH); return res.end(JSON.stringify({ ok: false, error: "Máy chủ ghi file khóa không được." })); }
      console.log("Khóa quản trị vừa được đặt lần đầu từ trang /admin.");
      res.writeHead(200, JSONH);
      res.end(JSON.stringify({ ok: true }));
    });
    return;
  }

  // (v91) DOI MA QUAN TRI — bat buoc nhap dung MA CU; ma luu tai
  // data/admin-key.txt. Neu may chu dung bien moi truong
  // DONATE_ADMIN_KEY thi khong doi qua trang nay duoc.
  if (p === "/api/admin/change-key") {
    const JSONH = { "Content-Type": "application/json; charset=utf-8", ...NO_STORE };
    if (req.method !== "POST") { res.writeHead(405, JSONH); return res.end(JSON.stringify({ ok: false, error: "Chỉ dùng POST." })); }
    if (process.env.DONATE_ADMIN_KEY) { res.writeHead(403, JSONH); return res.end(JSON.stringify({ ok: false, error: "Máy chủ đang dùng khóa từ biến môi trường DONATE_ADMIN_KEY — hãy sửa biến đó rồi khởi động lại máy chủ." })); }
    const adminKey = readAdminKey();
    if (!adminKey) { res.writeHead(403, JSONH); return res.end(JSON.stringify({ ok: false, error: "Máy chủ chưa có khóa quản trị — hãy đặt khóa lần đầu trước." })); }
    let body = "";
    req.on("data", (ch) => { body += ch; if (body.length > 4000) req.destroy(); });
    req.on("end", () => {
      let j;
      try { j = JSON.parse(body); } catch { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "Dữ liệu không hợp lệ." })); }
      const okk = j && typeof j.key === "string" ? j.key : "";
      const nk = j && typeof j.newKey === "string" ? j.newKey.trim() : "";
      if (okk !== adminKey) { res.writeHead(403, JSONH); return res.end(JSON.stringify({ ok: false, error: "Mã cũ không đúng." })); }
      if (nk.length < 4 || nk.length > 100) { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "Mã mới phải có từ 4 tới 100 ký tự." })); }
      try {
        fs.mkdirSync(DATA_DIR, { recursive: true });
        fs.writeFileSync(path.join(DATA_DIR, "admin-key.txt"), nk + "\n", "utf8");
      } catch { res.writeHead(500, JSONH); return res.end(JSON.stringify({ ok: false, error: "Máy chủ ghi file khóa không được." })); }
      console.log("Khóa quản trị vừa được ĐỔI từ trang /admin.");
      res.writeHead(200, JSONH);
      res.end(JSON.stringify({ ok: true }));
    });
    return;
  }

  if (p === "/api/admin/xmas") {
    const JSONH = { "Content-Type": "application/json; charset=utf-8", ...NO_STORE };
    if (req.method !== "POST") { res.writeHead(405, JSONH); return res.end(JSON.stringify({ ok: false, error: "Chỉ dùng POST." })); }
    const adminKey = readAdminKey();
    if (!adminKey) { res.writeHead(403, JSONH); return res.end(JSON.stringify({ ok: false, error: "Máy chủ chưa đặt khóa quản trị — chủ nhà thờ tạo file data/admin-key.txt chứa khóa riêng trước." })); }
    let body = "";
    req.on("data", (ch) => { body += ch; if (body.length > 4000) req.destroy(); });
    req.on("end", () => {
      let j;
      try { j = JSON.parse(body); } catch { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "Dữ liệu không hợp lệ." })); }
      if (!j || j.key !== adminKey) { res.writeHead(403, JSONH); return res.end(JSON.stringify({ ok: false, error: "Sai khóa quản trị." })); }
      xmasOnServer = j.on === true;
      saveXmasState();
      broadcast({ t: "xmas", on: xmasOnServer }, null);
      console.log("Chủ nhà thờ vừa " + (xmasOnServer ? "BẬT" : "TẮT") + " chế độ Giáng Sinh.");
      res.writeHead(200, JSONH);
      res.end(JSON.stringify({ ok: true, on: xmasOnServer }));
    });
    return;
  }

  // (v30) LƯU THÔNG TIN NGÂN HÀNG — chỉ chủ máy chủ (kèm khóa quản
  // trị): ghi data/donate-info.json để panel Tiền lễ hiển thị.
  if (p === "/api/admin/info") {
    const JSONH = { "Content-Type": "application/json; charset=utf-8", ...NO_STORE };
    if (req.method !== "POST") { res.writeHead(405, JSONH); return res.end(JSON.stringify({ ok: false, error: "Chỉ dùng POST." })); }
    const adminKey = readAdminKey();
    if (!adminKey) { res.writeHead(403, JSONH); return res.end(JSON.stringify({ ok: false, error: "Máy chủ chưa đặt khóa quản trị — chủ nhà thờ tạo file data/admin-key.txt chứa khóa riêng trước." })); }
    let body = "";
    req.on("data", (ch) => { body += ch; if (body.length > 20000) req.destroy(); });
    req.on("end", () => {
      let j;
      try { j = JSON.parse(body); } catch { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "Dữ liệu không hợp lệ." })); }
      if (!j || j.key !== adminKey) { res.writeHead(403, JSONH); return res.end(JSON.stringify({ ok: false, error: "Sai khóa quản trị." })); }
      const pick = (v) => (typeof v === "string" ? v.trim().slice(0, 90) : "");
      const info = { bank: pick(j.bank), account: pick(j.account), holder: pick(j.holder) };
      try {
        fs.mkdirSync(DATA_DIR, { recursive: true });
        fs.writeFileSync(path.join(DATA_DIR, "donate-info.json"), JSON.stringify(info));
      } catch { res.writeHead(500, JSONH); return res.end(JSON.stringify({ ok: false, error: "Máy chủ không ghi được thông tin." })); }
      console.log("Thông tin ngân hàng quyên góp vừa được chủ nhà thờ cập nhật.");
      res.writeHead(200, JSONH);
      res.end(JSON.stringify({ ok: true }));
    });
    return;
  }

  // (v29) THÔNG TIN NGÂN HÀNG của chủ máy chủ, hiện cùng mã QR trong
  // panel Tiền lễ: chủ nhà thờ đặt file data/donate-info.json với các
  // trường {bank, account, holder} trên máy chủ. Đây là thông tin
  // CÔNG KHAI để người quyên góp đối chiếu khi quét — chỉ đọc, không
  // có đường ghi qua HTTP. Chưa có file -> 404, panel chỉ hiện mã QR.
  if (p === "/donate-info") {
    return fs.readFile(path.join(DATA_DIR, "donate-info.json"), "utf8", (err, raw) => {
      if (err) { res.writeHead(404); return res.end("Chưa có thông tin ngân hàng"); }
      let j;
      try { j = JSON.parse(raw); } catch { res.writeHead(404); return res.end("Thông tin ngân hàng không hợp lệ"); }
      const pick = (v) => (typeof v === "string" ? v.trim().slice(0, 90) : "");
      const info = { bank: pick(j.bank), account: pick(j.account), holder: pick(j.holder) };
      if (!info.bank && !info.account && !info.holder) { res.writeHead(404); return res.end("Chưa có thông tin ngân hàng"); }
      res.writeHead(200, { "Content-Type": "application/json; charset=utf-8", ...NO_STORE });
      res.end(JSON.stringify(info));
    });
  }

  // (v28) MÃ QR QUYÊN GÓP (tiền lễ): ảnh QR của CHỦ NHÀ THỜ nằm trong
  // thư mục data/ (donate-qr.png / .jpg / .jpeg / .webp). Người chơi
  // chỉ TẢI XEM để quét bằng app ngân hàng — không có đường nào khác
  // đụng vào file này qua HTTP ngoài endpoint quản trị có khóa dưới đây.
  if (p === "/donate-qr") {
    const QR_EXTS = [[".png", "image/png"], [".jpg", "image/jpeg"], [".jpeg", "image/jpeg"], [".webp", "image/webp"]];
    let qi = 0;
    const tryQr = () => {
      if (qi >= QR_EXTS.length) { res.writeHead(404); return res.end("Chưa có mã QR"); }
      const [ext, mime] = QR_EXTS[qi++];
      fs.readFile(path.join(DATA_DIR, "donate-qr" + ext), (err, data) => {
        if (err) return tryQr();
        res.writeHead(200, { "Content-Type": mime, ...NO_STORE });
        res.end(data);
      });
    };
    return tryQr();
  }

  // (v28) ĐỔI MÃ QR — CHỈ CHỦ NHÀ THỜ: cần khóa quản trị lấy từ biến
  // môi trường DONATE_ADMIN_KEY hoặc file data/admin-key.txt do chủ
  // nhà thờ tự tạo trên máy chủ (file này KHÔNG nằm trong gói cài đặt,
  // người chơi không có, không thấy và không đoán được qua app).
  if (p === "/api/admin/qr") {
    const JSONH = { "Content-Type": "application/json; charset=utf-8", ...NO_STORE };
    if (req.method !== "POST") { res.writeHead(405, JSONH); return res.end(JSON.stringify({ ok: false, error: "Chỉ dùng POST." })); }
    const adminKey = readAdminKey();
    if (!adminKey) { res.writeHead(403, JSONH); return res.end(JSON.stringify({ ok: false, error: "Máy chủ chưa đặt khóa quản trị — chủ nhà thờ tạo file data/admin-key.txt chứa khóa riêng trước." })); }
    let body = "";
    req.on("data", (ch) => { body += ch; if (body.length > 4_500_000) req.destroy(); });
    req.on("end", () => {
      let j;
      try { j = JSON.parse(body); } catch { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "Dữ liệu không hợp lệ." })); }
      if (!j || j.key !== adminKey) { res.writeHead(403, JSONH); return res.end(JSON.stringify({ ok: false, error: "Sai khóa quản trị." })); }
      const m = /^data:image\/(png|jpeg|webp);base64,([A-Za-z0-9+/=]+)$/.exec(typeof j.dataUrl === "string" ? j.dataUrl : "");
      if (!m) { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "Ảnh phải là PNG, JPG hoặc WebP." })); }
      const buf = Buffer.from(m[2], "base64");
      if (!buf.length || buf.length > 3_000_000) { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "Ảnh trống hoặc quá lớn (tối đa 3MB)." })); }
      const ext = m[1] === "jpeg" ? ".jpg" : "." + m[1];
      try {
        fs.mkdirSync(DATA_DIR, { recursive: true });
        for (const e of [".png", ".jpg", ".jpeg", ".webp"]) { try { fs.unlinkSync(path.join(DATA_DIR, "donate-qr" + e)); } catch {} }
        fs.writeFileSync(path.join(DATA_DIR, "donate-qr" + ext), buf);
      } catch { res.writeHead(500, JSONH); return res.end(JSON.stringify({ ok: false, error: "Máy chủ không ghi được file QR." })); }
      console.log("Mã QR quyên góp vừa được chủ nhà thờ cập nhật.");
      res.writeHead(200, JSONH);
      res.end(JSON.stringify({ ok: true }));
    });
    return;
  }

  // (v87) TẠO ALBUM NHẠC — CHỈ CHỦ MÁY CHỦ: tạo thư mục con trong
  // public/music/ để thêm nhạc theo album riêng (theo yêu cầu của Trí).
  if (p === "/api/admin/album") {
    const JSONH = { "Content-Type": "application/json; charset=utf-8", ...NO_STORE };
    if (req.method !== "POST") { res.writeHead(405, JSONH); return res.end(JSON.stringify({ ok: false, error: "Chỉ dùng POST." })); }
    const adminKey = readAdminKey();
    if (!adminKey) { res.writeHead(403, JSONH); return res.end(JSON.stringify({ ok: false, error: "Máy chủ chưa đặt khóa quản trị — chủ nhà thờ tạo file data/admin-key.txt chứa khóa riêng trước." })); }
    let body = "";
    req.on("data", (ch) => { body += ch; if (body.length > 4000) req.destroy(); });
    req.on("end", () => {
      let j;
      try { j = JSON.parse(body); } catch { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "Dữ liệu không hợp lệ." })); }
      if (!j || j.key !== adminKey) { res.writeHead(403, JSONH); return res.end(JSON.stringify({ ok: false, error: "Sai khóa quản trị." })); }
      const album = String(j.name || "").split(/[\\/]/).pop().trim().replace(/[^\p{L}\p{N}._\-() ]/gu, "_").slice(0, 60);
      if (!album || album === "." || album === "..") { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "Tên album không hợp lệ." })); }
      try { fs.mkdirSync(path.join(MUSIC_DIR, album), { recursive: true }); }
      catch { res.writeHead(500, JSONH); return res.end(JSON.stringify({ ok: false, error: "Máy chủ không tạo được album." })); }
      console.log("Chủ nhà thờ vừa tạo album nhạc qua /admin: " + album);
      res.writeHead(200, JSONH);
      res.end(JSON.stringify({ ok: true, album: album }));
    });
    return;
  }

  // (v80) THÊM FILE NHẠC — CHỈ CHỦ MÁY CHỦ, từ trang /admin kèm
  // khóa quản trị (theo yêu cầu của Trí: chuyển việc thêm nhạc vào
  // trang quản lý): nhận file qua dataUrl base64 rồi ghi vào
  // public/music/ — đúng thư mục mà /api/music liệt kê cho trình
  // phát Thánh Ca. Định dạng .mp3/.ogg/.m4a theo đuôi tên file,
  // tối đa 100MB (v82 — Trí xin thêm dung lượng); tên file được làm sạch (bỏ ký tự lạ + đường dẫn).
  if (p === "/api/admin/music") {
    const JSONH = { "Content-Type": "application/json; charset=utf-8", ...NO_STORE };
    if (req.method !== "POST") { res.writeHead(405, JSONH); return res.end(JSON.stringify({ ok: false, error: "Chỉ dùng POST." })); }
    const adminKey = readAdminKey();
    if (!adminKey) { res.writeHead(403, JSONH); return res.end(JSON.stringify({ ok: false, error: "Máy chủ chưa đặt khóa quản trị — chủ nhà thờ tạo file data/admin-key.txt chứa khóa riêng trước." })); }
    let body = "";
    req.on("data", (ch) => { body += ch; if (body.length > 150_000_000) req.destroy(); });
    req.on("end", () => {
      let j;
      try { j = JSON.parse(body); } catch { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "Dữ liệu không hợp lệ." })); }
      if (!j || j.key !== adminKey) { res.writeHead(403, JSONH); return res.end(JSON.stringify({ ok: false, error: "Sai khóa quản trị." })); }
      let name = String(j.name || "").split(/[\\/]/).pop().trim().replace(/[^\p{L}\p{N}._\-() ]/gu, "_");
      if (!/\.(mp3|ogg|m4a)$/i.test(name) || name.startsWith(".")) { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "Tên file phải kết thúc bằng .mp3, .ogg hoặc .m4a." })); }
      const m = /^data:[^,]*;base64,([A-Za-z0-9+/=]+)$/.exec(typeof j.dataUrl === "string" ? j.dataUrl : "");
      if (!m) { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "Không đọc được dữ liệu file nhạc." })); }
      const buf = Buffer.from(m[1], "base64");
      if (!buf.length || buf.length > 100_000_000) { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "File trống hoặc quá lớn (tối đa 100MB)." })); }
      const albumRaw = String(j.album || "").split(/[\\/]/).pop().trim().replace(/[^\p{L}\p{N}._\-() ]/gu, "_").slice(0, 60);
      const album = (albumRaw && albumRaw !== "." && albumRaw !== "..") ? albumRaw : "";
      const destDir = album ? path.join(MUSIC_DIR, album) : MUSIC_DIR;
      try {
        fs.mkdirSync(destDir, { recursive: true });
        fs.writeFileSync(path.join(destDir, name), buf);
      } catch { res.writeHead(500, JSONH); return res.end(JSON.stringify({ ok: false, error: "Máy chủ không ghi được file nhạc." })); }
      const relName = album ? album + "/" + name : name;
      console.log("Chủ nhà thờ vừa thêm bài nhạc qua /admin: " + relName);
      broadcast({ t: "music" }, null); // (v84) báo MỌI client đang trong nhà thờ nạp lại danh sách nhạc ngay — không cần tải lại trang mới nghe được bài mới (yêu cầu của Trí)
      res.writeHead(200, JSONH);
      res.end(JSON.stringify({ ok: true, file: relName }));
    });
    return;
  }

  // (v80) XÓA FILE NHẠC — CHỈ CHỦ MÁY CHỦ, từ trang /admin kèm khóa
  // quản trị: xóa đúng một file trong public/music/ theo tên.
  if (p === "/api/admin/music-delete") {
    const JSONH = { "Content-Type": "application/json; charset=utf-8", ...NO_STORE };
    if (req.method !== "POST") { res.writeHead(405, JSONH); return res.end(JSON.stringify({ ok: false, error: "Chỉ dùng POST." })); }
    const adminKey = readAdminKey();
    if (!adminKey) { res.writeHead(403, JSONH); return res.end(JSON.stringify({ ok: false, error: "Máy chủ chưa đặt khóa quản trị — chủ nhà thờ tạo file data/admin-key.txt chứa khóa riêng trước." })); }
    let body = "";
    req.on("data", (ch) => { body += ch; if (body.length > 4000) req.destroy(); });
    req.on("end", () => {
      let j;
      try { j = JSON.parse(body); } catch { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "Dữ liệu không hợp lệ." })); }
      if (!j || j.key !== adminKey) { res.writeHead(403, JSONH); return res.end(JSON.stringify({ ok: false, error: "Sai khóa quản trị." })); }
      const parts = String(j.file || "").split(/[\\/]/).map((x) => x.trim()).filter((x) => x && x !== "." && x !== "..");
      const name = parts.slice(-2).join("/"); // (v87) cho phep ca "album/bai.mp3" (toi da MOT cap thu muc album)
      const fp = path.join(MUSIC_DIR, name);
      if (!parts.length || parts.length > 2 || !/\.(mp3|ogg|m4a)$/i.test(name) || !fp.startsWith(MUSIC_DIR + path.sep)) { res.writeHead(400, JSONH); return res.end(JSON.stringify({ ok: false, error: "Tên file nhạc không hợp lệ." })); }
      try { fs.unlinkSync(fp); } catch { res.writeHead(404, JSONH); return res.end(JSON.stringify({ ok: false, error: "Không xóa được — file có thể đã bị xóa rồi." })); }
      console.log("Chủ nhà thờ vừa xóa bài nhạc qua /admin: " + name);
      broadcast({ t: "music" }, null); // (v84) đồng bộ danh sách nhạc cho mọi client ngay khi xóa
      res.writeHead(200, JSONH);
      res.end(JSON.stringify({ ok: true }));
    });
    return;
  }

  // Danh sách nhạc thánh ca do người dùng tự chép vào public/music/
  if (p === "/api/music") {
    // (v87) liet ke ca nhac trong cac ALBUM (thu muc con cua public/music/): moi bai co them truong album ("" = thu muc goc); thu tu: bai goc truoc, roi tung album theo ten, trong album theo ten bai
    const list = [];
    try {
      const ents = fs.readdirSync(MUSIC_DIR, { withFileTypes: true });
      const pushF = (album, f) => { let sz = 0; try { sz = fs.statSync(path.join(MUSIC_DIR, album, f)).size; } catch (e) {} list.push({ file: album ? album + "/" + f : f, title: f.replace(/\.(mp3|ogg|m4a)$/i, ""), album: album, size: sz }); };
      ents.filter((d) => d.isFile() && /\.(mp3|ogg|m4a)$/i.test(d.name)).map((d) => d.name).sort().forEach((f) => pushF("", f));
      ents.filter((d) => d.isDirectory()).map((d) => d.name).sort().forEach((al) => {
        try { fs.readdirSync(path.join(MUSIC_DIR, al)).filter((f) => /\.(mp3|ogg|m4a)$/i.test(f)).sort().forEach((f) => pushF(al, f)); } catch (e) {}
      });
    } catch (e) {}
    res.writeHead(200, { "Content-Type": "application/json; charset=utf-8", ...NO_STORE });
    return res.end(JSON.stringify(list));
  }

  if (p === "/") p = "/index.html";
  const file = path.join(PUBLIC_DIR, p);
  if (!file.startsWith(PUBLIC_DIR)) { res.writeHead(403); return res.end("Forbidden"); }
  fs.stat(file, (err, st) => {
    if (err || !st.isFile()) { res.writeHead(404); return res.end("Not found"); }
    const type = MIME[path.extname(file)] || "application/octet-stream";
    const m = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range || "");
    if (m) {
      let start = m[1] === "" ? null : parseInt(m[1], 10);
      let end = m[2] === "" ? null : parseInt(m[2], 10);
      if (start === null) { start = Math.max(0, st.size - (end || 0)); end = st.size - 1; }
      else if (end === null || end >= st.size) end = st.size - 1;
      if (start > end || start >= st.size) { res.writeHead(416, { "Content-Range": "bytes */" + st.size }); return res.end(); }
      res.writeHead(206, { "Content-Type": type, "Content-Range": "bytes " + start + "-" + end + "/" + st.size, "Accept-Ranges": "bytes", "Content-Length": end - start + 1, ...NO_STORE });
      return fs.createReadStream(file, { start, end }).pipe(res);
    }
    res.writeHead(200, { "Content-Type": type, "Accept-Ranges": "bytes", "Content-Length": st.size, ...NO_STORE });
    fs.createReadStream(file).pipe(res);
  });
});

// ---------- WebSocket ----------
const wss = new WebSocketServer({ server: httpServer });
let nextId = 1;

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}
function broadcast(obj, exceptWs) {
  const msg = JSON.stringify(obj);
  for (const p of players.values()) {
    if (p.ws !== exceptWs && p.ws.readyState === p.ws.OPEN) p.ws.send(msg);
  }
}
function broadcastCounts() {
  broadcast({ t: "rooms", counts: { [CHURCH]: players.size } }, null);
}
function publicPlayer(p) {
  return { id: p.id, name: p.name, look: p.look, x: p.x, z: p.z, sit: !!p.sit, kneel: !!p.kneel, carry: !!p.carry, zone: p.zone || "nave", y: p.y || 0, ang: (typeof p.ang === "number" && isFinite(p.ang)) ? p.ang : Math.PI }; // (v41) ang = hướng mặt — snapshot cho người vào muộn dựng đúng tư thế ngồi/quỳ
}

wss.on("connection", (ws) => {
  const me = {
    id: "p" + nextId++, ws, name: "Khách", look: cleanLook({}),
    x: SPAWN.x, z: SPAWN.z, sit: false, kneel: false, carry: false, zone: "nave", y: 0, joined: false, lastSign: 0, lastPrayer: 0, lastEmote: 0, lastChat: 0,
  };
  ws.isAlive = true;
  ws.on("pong", () => (ws.isAlive = true));

  // giới hạn tốc độ gửi tin đơn giản: tối đa ~120 tin / 3 giây
  let windowStart = Date.now();
  let windowCount = 0;

  ws.on("message", (raw) => {
    const now = Date.now();
    if (now - windowStart > 3000) { windowStart = now; windowCount = 0; }
    if (++windowCount > 120) return;

    let m;
    try { m = JSON.parse(raw.toString()); } catch { return; }
    if (!m || typeof m.t !== "string") return;

    if (m.t === "join") {
      me.name = cleanText(m.name, MAX_NAME) || "Khách";
      me.look = cleanLook(m.look);
      me.x = SPAWN.x;
      me.z = SPAWN.z;
      me.sit = false;
      me.kneel = false;
      me.carry = false;
      me.zone = "nave"; // (v39) xuất hiện hẳn trong nhà thờ
      me.y = 0;
      me.joined = true;
      players.set(me.id, me);
      send(ws, {
        t: "welcome",
        id: me.id,
        church: CHURCH,
        players: [...players.values()].filter((p) => p.id !== me.id).map(publicPlayer),
        candles,
        xmas: xmasOnServer, // (v65) trạng thái Giáng Sinh toàn máy chủ
      });
      broadcast({ t: "join", player: publicPlayer(me) }, ws);
      broadcastCounts();
      return;
    }

    if (!me.joined) return; // chưa join thì bỏ qua mọi tin khác

    if (m.t === "move") {
      me.x = clamp(m.x, -BOUND_X, BOUND_X, me.x);
      me.z = clamp(m.z, BOUND_Z_MIN, BOUND_Z_MAX, me.z);
      me.sit = !!m.sit;
      me.kneel = !!m.kneel && !me.sit; // ngồi ghế và quỳ loại trừ lẫn nhau — đang ngồi thì không quỳ
      me.zone = cleanZone(m.zone, me.zone); // (v35) zone lạ -> giữ zone hiện tại (không sập, không nhảy khu)
      me.y = clamp(m.y, -45, 12, me.y); // (v35) cao độ sàn: 0 tầng trệt, ~5,2 gác đàn
      if (typeof m.ang === "number" && isFinite(m.ang)){ let a = m.ang % (Math.PI * 2); if (a > Math.PI) a -= Math.PI * 2; if (a < -Math.PI) a += Math.PI * 2; me.ang = a; } // (v41) hướng mặt đi kèm state tư thế
      broadcast({ t: "move", id: me.id, x: me.x, z: me.z, sit: me.sit, kneel: me.kneel, zone: me.zone, y: me.y, ang: me.ang }, ws);
    } else if (m.t === "look") {
      me.look = cleanLook(m.look);
      // (v17) tin look có thể kèm TÊN mới (người chơi đổi tên ở màn hình
      // chờ rồi bấm Vào nhà thờ) — cập nhật + phát kèm để mọi người thấy
      // đúng tên; thiếu name thì giữ tên cũ.
      if (typeof m.name === "string" && m.name.trim()) me.name = cleanText(m.name, MAX_NAME);
      broadcast({ t: "look", id: me.id, look: me.look, name: me.name }, ws);
    } else if (m.t === "carry") {
      // (v12) trạng thái CẦM NẾN của một người: client báo on=true ngay khi
      // hoạt cảnh lấy nến từ giỏ hoàn tất (nến lên tay), on=false khi ấn nút
      // "✕ Hủy thắp nến". Server chỉ lưu cờ (để người vào sau thấy ngay qua
      // welcome/join) và chuyển tiếp cho mọi người còn lại.
      me.carry = !!m.on;
      broadcast({ t: "carry", id: me.id, on: me.carry }, ws);
    } else if (m.t === "candle") {
      // v8: luồng thắp nến mới KHÔNG còn thẻ ý nguyện — client gửi intention
      // trống; server điền lời mặc định trang nghiêm để nến vẫn có chữ khi
      // người khác chạm vào xem, giao thức giữ nguyên.
      const intention = cleanText(m.intention, MAX_INTENTION) || "Một ngọn nến cầu nguyện";
      const candle = { id: "c" + Date.now().toString(36) + Math.floor(Math.random() * 1e4), intention, name: me.name, ts: Date.now(), st: cleanCandleSt(m.st) };
      candles.push(candle);
      if (candles.length > MAX_CANDLES) candles.splice(0, candles.length - MAX_CANDLES);
      saveCandles();
      broadcast({ t: "candle", candle }, null); // gửi cả người thắp để đồng bộ id
    } else if (m.t === "sign") {
      // chống spam riêng: mỗi người tối đa 1 lần / 3 giây (ngoài cửa sổ rate limit chung)
      if (now - me.lastSign < 3000) return;
      me.lastSign = now;
      broadcast({ t: "sign", id: me.id }, ws);
    } else if (m.t === "prayer") {
      // (v11) DÂNG LỜI CẦU NGUYỆN CHUNG: client của người dâng đã chạy hiệu
      // ứng bay lên ngay tại máy mình; tin này để MỌI NGƯỜI trong phòng cùng
      // thấy dòng chữ bay lên tại avatar của người dâng. Chuỗi rỗng -> bỏ
      // qua; chống spam riêng 8 giây/người (ngoài cửa sổ rate limit chung).
      const text = cleanText(m.text, MAX_PRAYER);
      if (!text) return;
      if (now - me.lastPrayer < PRAYER_SPAM_MS) return;
      me.lastPrayer = now;
      broadcast({ t: "prayer", id: me.id, name: me.name, text, zone: me.zone || "nave" }, null); // gửi cả người dâng — client chặn echo theo id; (v35) zone lấy từ trạng thái server của người dâng
    } else if (m.t === "soulPrayer") {
      // (v51) DÂNG LỜI NGUYỆN CHO CÁC LINH HỒN ở góc cầu nguyện tầng
      // hầm: client của MỌI người chơi (mọi zone) hiện banner chung.
      // (v53) Panel đã BỎ ô tên linh hồn theo yêu cầu của Trí — nay
      // NỘI DUNG lời nguyện là phần bắt buộc (rỗng -> bỏ qua). Chống
      // spam dùng CHUNG cửa sổ 8 giây với prayer (me.lastPrayer).
      const text = cleanText(m.text, MAX_PRAYER);
      if (!text) return;
      if (now - me.lastPrayer < PRAYER_SPAM_MS) return;
      me.lastPrayer = now;
      broadcast({ t: "soulPrayer", id: me.id, name: me.name, text }, null); // gửi cả người dâng — client chặn echo theo id
    } else if (m.t === "emote") {
      // (v28) TƯƠNG TÁC GIỮA NGƯỜI CHƠI: vẫy chào (wave) / cầu nguyện
      // cho (pray) một người cụ thể trong phòng. Server chỉ chuyển
      // tiếp cho NGƯỜI KHÁC (người gửi đã tự chạy hiệu ứng tại máy
      // mình); người nhận hiện bong bóng trên đầu người gửi, riêng
      // người được nhắm (to) còn nhận thêm thông báo nhỏ. Chống spam
      // riêng 2,5 giây/người; to phải là người đang ở trong phòng.
      const kind = ["wave", "pray", "handshake", "bless"].includes(m.kind) ? m.kind : null; // (v41) còn 4 tương tác: vẫy chào / cầu nguyện cho / bắt tay / chúc bình an (đã gỡ tặng hoa, thả tim, tặng tràng hạt) — vẫn chống spam chung 2,5s
      if (!kind) return;
      if (typeof m.to !== "string" || !players.has(m.to) || m.to === me.id) return;
      if (now - me.lastEmote < 2500) return;
      me.lastEmote = now;
      broadcast({ t: "emote", id: me.id, name: me.name, kind, to: m.to }, ws);
    } else if (m.t === "chat") {
      // (v28) TRÒ CHUYỆN TRONG PHÒNG: chuyển tiếp cho CẢ phòng (gồm
      // người gửi — client hiển thị bong bóng + nhật ký theo echo để
      // đồng bộ thứ tự). Tối đa 200 ký tự, chống spam 1,2 giây/tin.
      const text = cleanText(m.text, MAX_CHAT);
      if (!text) return;
      if (now - me.lastChat < 1200) return;
      me.lastChat = now;
      broadcast({ t: "chat", id: me.id, name: me.name, text }, null);
    }
  });

  ws.on("close", () => {
    if (me.joined) {
      players.delete(me.id);
      broadcast({ t: "leave", id: me.id }, ws);
      broadcastCounts();
    }
  });
});

// dọn kết nối chết
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);

httpServer.listen(PORT, () => {
  console.log(`Nhà Thờ Online 3D đang chạy tại http://localhost:${PORT} (nhà thờ: ${CHURCH})`);
  if (!readAdminKey()) console.log("Trang quản trị /admin chưa có khóa — mở http://localhost:" + PORT + "/admin để đặt khóa quản trị lần đầu."); // (v82)
});
