package genesis

import (
	"encoding/json"
	"os"

	"github.com/harmony-one/harmony/internal/utils"
)

// LocalHarmonyAccounts are the accounts for the initial genesis nodes used for local test.
var LocalHarmonyAccounts = []DeployAccount{
	{Index: " 0 ", Address: "one1pdv9lrdwl0rg5vglh4xtyrv3wjk3wsqket7zxy", BLSPublicKey: "65f55eb3052f9e9f632b2923be594ba77c55543f5c58ee1454b9cfd658d25e06373b0f7d42a19c84768139ea294f6204"},
	{Index: " 1 ", Address: "one1m6m0ll3q7ljdqgmth2t5j7dfe6stykucpj2nr5", BLSPublicKey: "40379eed79ed82bebfb4310894fd33b6a3f8413a78dc4d43b98d0adc9ef69f3285df05eaab9f2ce5f7227f8cb920e809"},
	{Index: " 2 ", Address: "one12fuf7x9rgtdgqg7vgq0962c556m3p7afsxgvll", BLSPublicKey: "02c8ff0b88f313717bc3a627d2f8bb172ba3ad3bb9ba3ecb8eed4b7c878653d3d4faf769876c528b73f343967f74a917"},
	{Index: " 3 ", Address: "one16qsd5ant9v94jrs89mruzx62h7ekcfxmduh2rx", BLSPublicKey: "ee2474f93cba9241562efc7475ac2721ab0899edf8f7f115a656c0c1f9ef8203add678064878d174bb478fa2e6630502"},
	{Index: " 4 ", Address: "one1pf75h0t4am90z8uv3y0dgunfqp4lj8wr3t5rsp", BLSPublicKey: "e751ec995defe4931273aaebcb2cd14bf37e629c554a57d3f334c37881a34a6188a93e76113c55ef3481da23b7d7ab09"},
	{Index: " 5 ", Address: "one1est2gxcvavmtnzc7mhd73gzadm3xxcv5zczdtw", BLSPublicKey: "776f3b8704f4e1092a302a60e84f81e476c212d6f458092b696df420ea19ff84a6179e8e23d090b9297dc041600bc100"},
	{Index: " 6 ", Address: "one1spshr72utf6rwxseaz339j09ed8p6f8ke370zj", BLSPublicKey: "2d61379e44a772e5757e27ee2b3874254f56073e6bd226eb8b160371cc3c18b8c4977bd3dcb71fd57dc62bf0e143fd08"},
	{Index: " 7 ", Address: "one1a0x3d6xpmr6f8wsyaxd9v36pytvp48zckswvv9", BLSPublicKey: "c4e4708b6cf2a2ceeb59981677e9821eebafc5cf483fb5364a28fa604cc0ce69beeed40f3f03815c9e196fdaec5f1097"},
	{Index: " 8 ", Address: "one1d2rngmem4x2c6zxsjjz29dlah0jzkr0k2n88wc", BLSPublicKey: "86dc2fdc2ceec18f6923b99fd86a68405c132e1005cf1df72dca75db0adfaeb53d201d66af37916d61f079f34f21fb96"},
	{Index: " 9 ", Address: "one1658znfwf40epvy7e46cqrmzyy54h4n0qa73nep", BLSPublicKey: "49d15743b36334399f9985feb0753430a2b287b2d68b84495bbb15381854cbf01bca9d1d9f4c9c8f18509b2bfa6bd40f"},
}

// LocalFnAccounts are the accounts for the initial FN used for local test.
var LocalFnAccounts = []DeployAccount{
	{Index: " 0 ", Address: "one1a50tun737ulcvwy0yvve0pvu5skq0kjargvhwe", BLSPublicKey: "52ecce5f64db21cbe374c9268188f5d2cdd5bec1a3112276a350349860e35fb81f8cfe447a311e0550d961cf25cb988d"},
	{Index: " 1 ", Address: "one1uyshu2jgv8w465yc8kkny36thlt2wvel89tcmg", BLSPublicKey: "a547a9bf6fdde4f4934cde21473748861a3cc0fe8bbb5e57225a29f483b05b72531f002f8187675743d819c955a86100"},
	{Index: " 2 ", Address: "one103q7qe5t2505lypvltkqtddaef5tzfxwsse4z7", BLSPublicKey: "678ec9670899bf6af85b877058bea4fc1301a5a3a376987e826e3ca150b80e3eaadffedad0fedfa111576fa76ded980c"},
	{Index: " 3 ", Address: "one129r9pj3sk0re76f7zs3qz92rggmdgjhtwge62k", BLSPublicKey: "63f479f249c59f0486fda8caa2ffb247209489dae009dfde6144ff38c370230963d360dffd318cfb26c213320e89a512"},
}

// LocalHarmonyAccountsV1 are the accounts for the initial genesis nodes used for local test.
var LocalHarmonyAccountsV1 = []DeployAccount{
	{Index: " 0 ", Address: "one1pdv9lrdwl0rg5vglh4xtyrv3wjk3wsqket7zxy", BLSPublicKey: "65f55eb3052f9e9f632b2923be594ba77c55543f5c58ee1454b9cfd658d25e06373b0f7d42a19c84768139ea294f6204"},
	{Index: " 1 ", Address: "one1m6m0ll3q7ljdqgmth2t5j7dfe6stykucpj2nr5", BLSPublicKey: "40379eed79ed82bebfb4310894fd33b6a3f8413a78dc4d43b98d0adc9ef69f3285df05eaab9f2ce5f7227f8cb920e809"},
	{Index: " 2 ", Address: "one12fuf7x9rgtdgqg7vgq0962c556m3p7afsxgvll", BLSPublicKey: "02c8ff0b88f313717bc3a627d2f8bb172ba3ad3bb9ba3ecb8eed4b7c878653d3d4faf769876c528b73f343967f74a917"},
	{Index: " 3 ", Address: "one16qsd5ant9v94jrs89mruzx62h7ekcfxmduh2rx", BLSPublicKey: "ee2474f93cba9241562efc7475ac2721ab0899edf8f7f115a656c0c1f9ef8203add678064878d174bb478fa2e6630502"},
	{Index: " 4 ", Address: "one1pf75h0t4am90z8uv3y0dgunfqp4lj8wr3t5rsp", BLSPublicKey: "e751ec995defe4931273aaebcb2cd14bf37e629c554a57d3f334c37881a34a6188a93e76113c55ef3481da23b7d7ab09"},
	{Index: " 5 ", Address: "one1est2gxcvavmtnzc7mhd73gzadm3xxcv5zczdtw", BLSPublicKey: "776f3b8704f4e1092a302a60e84f81e476c212d6f458092b696df420ea19ff84a6179e8e23d090b9297dc041600bc100"},
	{Index: " 6 ", Address: "one1spshr72utf6rwxseaz339j09ed8p6f8ke370zj", BLSPublicKey: "2d61379e44a772e5757e27ee2b3874254f56073e6bd226eb8b160371cc3c18b8c4977bd3dcb71fd57dc62bf0e143fd08"},
	{Index: " 7 ", Address: "one1a0x3d6xpmr6f8wsyaxd9v36pytvp48zckswvv9", BLSPublicKey: "c4e4708b6cf2a2ceeb59981677e9821eebafc5cf483fb5364a28fa604cc0ce69beeed40f3f03815c9e196fdaec5f1097"},
	{Index: " 8 ", Address: "one1d2rngmem4x2c6zxsjjz29dlah0jzkr0k2n88wc", BLSPublicKey: "86dc2fdc2ceec18f6923b99fd86a68405c132e1005cf1df72dca75db0adfaeb53d201d66af37916d61f079f34f21fb96"},
	{Index: " 9 ", Address: "one1658znfwf40epvy7e46cqrmzyy54h4n0qa73nep", BLSPublicKey: "49d15743b36334399f9985feb0753430a2b287b2d68b84495bbb15381854cbf01bca9d1d9f4c9c8f18509b2bfa6bd40f"},
}

// LocalFnAccountsV1 are the accounts for the initial FN used for local test.
var LocalFnAccountsV1 = []DeployAccount{
	{Index: " 0 ", Address: "one1a50tun737ulcvwy0yvve0pvu5skq0kjargvhwe", BLSPublicKey: "52ecce5f64db21cbe374c9268188f5d2cdd5bec1a3112276a350349860e35fb81f8cfe447a311e0550d961cf25cb988d"},
	{Index: " 1 ", Address: "one1uyshu2jgv8w465yc8kkny36thlt2wvel89tcmg", BLSPublicKey: "a547a9bf6fdde4f4934cde21473748861a3cc0fe8bbb5e57225a29f483b05b72531f002f8187675743d819c955a86100"},
	{Index: " 2 ", Address: "one103q7qe5t2505lypvltkqtddaef5tzfxwsse4z7", BLSPublicKey: "678ec9670899bf6af85b877058bea4fc1301a5a3a376987e826e3ca150b80e3eaadffedad0fedfa111576fa76ded980c"},
	{Index: " 3 ", Address: "one129r9pj3sk0re76f7zs3qz92rggmdgjhtwge62k", BLSPublicKey: "63f479f249c59f0486fda8caa2ffb247209489dae009dfde6144ff38c370230963d360dffd318cfb26c213320e89a512"},
	{Index: " 4 ", Address: "one1d2rngmem4x2c6zxsjjz29dlah0jzkr0k2n88wc", BLSPublicKey: "16513c487a6bb76f37219f3c2927a4f281f9dd3fd6ed2e3a64e500de6545cf391dd973cc228d24f9bd01efe94912e714"},
	{Index: " 5 ", Address: "one1658znfwf40epvy7e46cqrmzyy54h4n0qa73nep", BLSPublicKey: "576d3c48294e00d6be4a22b07b66a870ddee03052fe48a5abbd180222e5d5a1f8946a78d55b025de21635fd743bbad90"},
}

// LocalHarmonyAccountsV2 are the accounts for the initial genesis nodes used for local test.
var LocalHarmonyAccountsV2 = []DeployAccount{
	{Index: " 0 ", Address: "one1pdv9lrdwl0rg5vglh4xtyrv3wjk3wsqket7zxy", BLSPublicKey: "65f55eb3052f9e9f632b2923be594ba77c55543f5c58ee1454b9cfd658d25e06373b0f7d42a19c84768139ea294f6204"},
	{Index: " 1 ", Address: "one1m6m0ll3q7ljdqgmth2t5j7dfe6stykucpj2nr5", BLSPublicKey: "40379eed79ed82bebfb4310894fd33b6a3f8413a78dc4d43b98d0adc9ef69f3285df05eaab9f2ce5f7227f8cb920e809"},
	{Index: " 2 ", Address: "one12fuf7x9rgtdgqg7vgq0962c556m3p7afsxgvll", BLSPublicKey: "02c8ff0b88f313717bc3a627d2f8bb172ba3ad3bb9ba3ecb8eed4b7c878653d3d4faf769876c528b73f343967f74a917"},
	{Index: " 3 ", Address: "one16qsd5ant9v94jrs89mruzx62h7ekcfxmduh2rx", BLSPublicKey: "ee2474f93cba9241562efc7475ac2721ab0899edf8f7f115a656c0c1f9ef8203add678064878d174bb478fa2e6630502"},
	{Index: " 4 ", Address: "one1pf75h0t4am90z8uv3y0dgunfqp4lj8wr3t5rsp", BLSPublicKey: "e751ec995defe4931273aaebcb2cd14bf37e629c554a57d3f334c37881a34a6188a93e76113c55ef3481da23b7d7ab09"},
	{Index: " 5 ", Address: "one1est2gxcvavmtnzc7mhd73gzadm3xxcv5zczdtw", BLSPublicKey: "776f3b8704f4e1092a302a60e84f81e476c212d6f458092b696df420ea19ff84a6179e8e23d090b9297dc041600bc100"},
	{Index: " 6 ", Address: "one1spshr72utf6rwxseaz339j09ed8p6f8ke370zj", BLSPublicKey: "2d61379e44a772e5757e27ee2b3874254f56073e6bd226eb8b160371cc3c18b8c4977bd3dcb71fd57dc62bf0e143fd08"},
	{Index: " 7 ", Address: "one1a0x3d6xpmr6f8wsyaxd9v36pytvp48zckswvv9", BLSPublicKey: "c4e4708b6cf2a2ceeb59981677e9821eebafc5cf483fb5364a28fa604cc0ce69beeed40f3f03815c9e196fdaec5f1097"},
	{Index: " 8 ", Address: "one1d2rngmem4x2c6zxsjjz29dlah0jzkr0k2n88wc", BLSPublicKey: "86dc2fdc2ceec18f6923b99fd86a68405c132e1005cf1df72dca75db0adfaeb53d201d66af37916d61f079f34f21fb96"},
	{Index: " 9 ", Address: "one1658znfwf40epvy7e46cqrmzyy54h4n0qa73nep", BLSPublicKey: "49d15743b36334399f9985feb0753430a2b287b2d68b84495bbb15381854cbf01bca9d1d9f4c9c8f18509b2bfa6bd40f"},
	{Index: " 10 ", Address: "one1z05g55zamqzfw9qs432n33gycdmyvs38xjemyl", BLSPublicKey: "95117937cd8c09acd2dfae847d74041a67834ea88662a7cbed1e170350bc329e53db151e5a0ef3e712e35287ae954818"},
	{Index: " 11 ", Address: "one1ljznytjyn269azvszjlcqvpcj6hjm822yrcp2e", BLSPublicKey: "68ae289d73332872ec8d04ac256ca0f5453c88ad392730c5741b6055bc3ec3d086ab03637713a29f459177aaa8340615"},
}

// LocalFnAccountsV2 are the accounts for the initial FN used for local test.
var LocalFnAccountsV2 = []DeployAccount{
	{Index: " 0 ", Address: "one1a50tun737ulcvwy0yvve0pvu5skq0kjargvhwe", BLSPublicKey: "52ecce5f64db21cbe374c9268188f5d2cdd5bec1a3112276a350349860e35fb81f8cfe447a311e0550d961cf25cb988d"},
	{Index: " 1 ", Address: "one1uyshu2jgv8w465yc8kkny36thlt2wvel89tcmg", BLSPublicKey: "a547a9bf6fdde4f4934cde21473748861a3cc0fe8bbb5e57225a29f483b05b72531f002f8187675743d819c955a86100"},
	{Index: " 2 ", Address: "one103q7qe5t2505lypvltkqtddaef5tzfxwsse4z7", BLSPublicKey: "678ec9670899bf6af85b877058bea4fc1301a5a3a376987e826e3ca150b80e3eaadffedad0fedfa111576fa76ded980c"},
	{Index: " 3 ", Address: "one129r9pj3sk0re76f7zs3qz92rggmdgjhtwge62k", BLSPublicKey: "63f479f249c59f0486fda8caa2ffb247209489dae009dfde6144ff38c370230963d360dffd318cfb26c213320e89a512"},
	{Index: " 4 ", Address: "one1d2rngmem4x2c6zxsjjz29dlah0jzkr0k2n88wc", BLSPublicKey: "16513c487a6bb76f37219f3c2927a4f281f9dd3fd6ed2e3a64e500de6545cf391dd973cc228d24f9bd01efe94912e714"},
	{Index: " 5 ", Address: "one1658znfwf40epvy7e46cqrmzyy54h4n0qa73nep", BLSPublicKey: "576d3c48294e00d6be4a22b07b66a870ddee03052fe48a5abbd180222e5d5a1f8946a78d55b025de21635fd743bbad90"},
	{Index: " 6 ", Address: "one1ghkz3frhske7emk79p7v2afmj4a5t0kmjyt4s5", BLSPublicKey: "eca09c1808b729ca56f1b5a6a287c6e1c3ae09e29ccf7efa35453471fcab07d9f73cee249e2b91f5ee44eb9618be3904"},
	{Index: " 7 ", Address: "one1d7jfnr6yraxnrycgaemyktkmhmajhp8kl0yahv", BLSPublicKey: "f47238daef97d60deedbde5302d05dea5de67608f11f406576e363661f7dcbc4a1385948549b31a6c70f6fde8a391486"},
	{Index: " 8 ", Address: "one1r4zyyjqrulf935a479sgqlpa78kz7zlcg2jfen", BLSPublicKey: "fc4b9c535ee91f015efff3f32fbb9d32cdd9bfc8a837bb3eee89b8fff653c7af2050a4e147ebe5c7233dc2d5df06ee0a"},
	{Index: " 9 ", Address: "one1p7ht2d4kl8ve7a8jxw746yfnx4wnfxtp8jqxwe", BLSPublicKey: "ca86e551ee42adaaa6477322d7db869d3e203c00d7b86c82ebee629ad79cb6d57b8f3db28336778ec2180e56a8e07296"},
}

var ExprHarmonyAccounts = []DeployAccount{
	{Index: " 0 ", Address: "one13c9xmt4f73t8ct73csrmnptz6ydd2t9fx08llk", EthAddr: "0x8e0a6daEA9f4567C2FD1c407b98562D11AD52Ca9", BLSPublicKey: "9035f9fafa7afe0d7237de9becff06365aaefdd46f937489354c86d4a72b485e386e25ec2b5bbfcc2dec5b1bb90ac711", ShardID: 0},
	{Index: " 1 ", Address: "one14l04hctqxeyvcc2s2f9lcdry2j7sstaafjw9qc", EthAddr: "0xAfDf5Be1603648cC6150524Bfc346454bD082fBD", BLSPublicKey: "6cfd08c4bf6dd6d8a4869816368f09a8f09c1146a05c268a4948e11e8f9d070f5b831db1c710ba0924d8f2b466700b99", ShardID: 0},
	{Index: " 2 ", Address: "one15lqeqw3pzx0j7ye7wajhd6pwcyrjlsj3u7wh6d", EthAddr: "0xa7c1903A21119F2f133e776576e82ec1072Fc251", BLSPublicKey: "c283dfd0e94be594b1a83a0369fc531cfc1b40974abe86513aa81c10936e69204cf25e3ac249345d0de50f8677d51084", ShardID: 0},
	{Index: " 3 ", Address: "one17v5pd92zqyefurfw4kmqsctuep3f9rsr8zxcxm", EthAddr: "0xF32816954201329E0d2eaDb608617cC862928E03", BLSPublicKey: "b84716b146b54a390ab7df75e8e2c319e178134830a1b945c976aba4db9077018c2557df408a351791dbcecfbb2cf68f", ShardID: 0},
	{Index: " 4 ", Address: "one1a9haawcgnz82sz5cvczdqatlp86rd8mdmlr0gx", EthAddr: "0xE96fdeBb08988ea80A986604D0757f09f4369F6D", BLSPublicKey: "be11a0cadfa884630ed63cca67b0e18e6c5823db36d95ce3b2b0bb47a6ae2d0c30b7e1015fb9c44a730172e8d9e1698d", ShardID: 0},
	{Index: " 5 ", Address: "one1ccru9g306yuaca09gj2n9rwly4mzqsq3v2a2yh", EthAddr: "0xc607C2a22FD139DC75e54495328dDf2576204011", BLSPublicKey: "16a7601a4b7e683fdd5545ac89bd3e57bee18c24cf00d882652489ec19abc74135d6b318579e41727c75f8edc7d41291", ShardID: 0},
	{Index: " 6 ", Address: "one1dlccw06cym5lsqm9d92eujgw5kdc8ffxjqgvg8", EthAddr: "0x6ff1873f5826e9f8036569559e490ea59b83a526", BLSPublicKey: "2a6d5cad2dc21db654b29028dce9ae26a3ffe07a2006f4c8ac73f5f8499e58e3912a6c471e69ce34b2ce3bc437ebd683", ShardID: 0},
	{Index: " 7 ", Address: "one1pkvyvdpk888z97pppqk5stdnh4y6w3kjeg0a9c", EthAddr: "0x0D9846343639cE22F821082D482DB3bD49a746d2", BLSPublicKey: "9d391576ecd8c16918601e70f8621cf7492b9e33b2b08ed85446fb06b9a478aa165acc3b3a1a6aee46ea01fec7b58d0f", ShardID: 0},
	{Index: " 8 ", Address: "one1qy8h25sfayfeyljwpm78kgn5vvhycz4nfp7t4z", EthAddr: "0x010F755209e913927e4E0efC7B2274632E4C0ab3", BLSPublicKey: "702698122e486555acceb821ea201827308f89f50c5c5a7c7f71a152ee03896c756bceeb5fd43e40525efcf82a7e3606", ShardID: 0},
	{Index: " 9 ", Address: "one1s86mpvhp3lm3jy7len4xcxsl756p9kx256y8vu", EthAddr: "0x81F5B0B2E18ff71913DFCCeA6C1a1FF53412d8cA", BLSPublicKey: "ad282a42d1130ff5b1f01a413d0865e91043cd7c0377640dddc839d9f8091acb691947378a0d615c35368d8a7248b60a", ShardID: 0},
	{Index: " 10 ", Address: "one1sfguxxxzc3fvual0cauczlq0lgw4jtfjthldpg", EthAddr: "0x8251c318c2C452ce77eFc779817C0FfA1D592D32", BLSPublicKey: "e69ec0cc8410476b2a1d6d4fe065ff0ba5828e46107f0f193b40e9800bdd899ceca8cb346a30724eb5094180dfc2540f", ShardID: 0},
	{Index: " 11 ", Address: "one1udj8m4e2vk9w4s8us954rpkwy8mh4zg4zktned", EthAddr: "0xE3647Dd72a658aeAC0FC81695186CE21f77A8915", BLSPublicKey: "1c0fe2abae814d448cb0de4c15594928a6e34dd6a3f179f025db6079f7c70e8277eea539cf5c2f548826d1c5d7f34f10", ShardID: 0},
	{Index: " 12 ", Address: "one1vrjgynvwmgxccyxlcry2v247n92xxkjm0edfv7", EthAddr: "0x60e4824d8edA0D8C10Dfc0C8a62abE9954635A5B", BLSPublicKey: "a6cdb4140a1c91744a309bd1a8b90ca0534ec9f3df3fdeba99514e5bfe98b2ab9e84033cc4d6a28e78bb208181041c04", ShardID: 0},
	{Index: " 13 ", Address: "one1yat3cy3kjfv0gs0g362jrv50e7d6pafm2m07qz", EthAddr: "0x27571C12369258F441e88e9521B28fcf9BA0f53B", BLSPublicKey: "c79eca62a6a26807ef6932603a27bf0be2e0c004f7ab920817a6c6ed7c3bcd2a61b6bbc8b6dd6374058cca7eb3200a13", ShardID: 0},
	{Index: " 14 ", Address: "one105clp5lulaqq96x64cvjt5fw2yh0he0frdlqdj", EthAddr: "0x7d31F0d3fCFF4002e8dAae1925d12e512efbe5E9", BLSPublicKey: "4baf0b53e9c9c70c67a57dd68ce1d955424d7253f5be572d3872730c533233373ed435c054aacdd591a1aef8757d3508", ShardID: 1},
	{Index: " 15 ", Address: "one124t5xspw7gjnl5gs3d9063qeysnfuvp2jtv0j2", EthAddr: "0x555743402Ef2253fD1108B4aFd441924269E302a", BLSPublicKey: "2abe6fb3c46661cc7c7d4c169236f3430e925e604c0c49e5bfc11c5b497851608e4e735851a307c63a0099474bc7760b", ShardID: 1},
	{Index: " 16 ", Address: "one12h20hukrqsqwplwj99jk8jg9mza0804g0asmax", EthAddr: "0x55d4fBF2C30400E0fdD2296563C905d8Baf3bea8", BLSPublicKey: "58bf5e28390b3fcb258a59c9ad4ecb5bff7b79d48bf96bdf9bb62e5b6b738305af96bee4284b923238277e539b52a70f", ShardID: 1},
	{Index: " 17 ", Address: "one14mtgwx6w20h49kx478zmjxre52vn68x4k42dqz", EthAddr: "0xAEd6871b4E53EF52d8d5f1c5b91879A2993d1CD5", BLSPublicKey: "0839ec9903bf3ccc2864f144e26bfb6889aeadfe7bd38061d28e8bd76e1d219bc0c9a5bd3b080fdf452a691fe891240c", ShardID: 1},
	{Index: " 18 ", Address: "one18z8msmwpdyv785k53hhx7apq9k3tfd4e0tzngc", EthAddr: "0x388fB86dC16919e3D2D48Dee6F74202Da2b4b6B9", BLSPublicKey: "26db0b37d6af3ce5109fc726e7040b881addc0810f5f80c5accb27103562570e2d7ba2069369c81705a7792f61cc6709", ShardID: 1},
	{Index: " 19 ", Address: "one1a08etj5n048fypmzl9srgu59fl8r6l52eek6z2", EthAddr: "0xeBcf95CA937d4e920762f9603472854fcE3d7E8A", BLSPublicKey: "27afaaa1971ef6dd17c78894b8e095bd9e4a57fcdbf48ba0854c05903d3d8aca75e4d6305f42258d3e6abb1286dd100d", ShardID: 1},
	{Index: " 20 ", Address: "one1a22c8uxzgx5lflq32xtxtpv62jf0df9umtl0ml", EthAddr: "0xEa9583f0C241a9F4FC11519665859a5492f6a4Bc", BLSPublicKey: "cf588d4e30874c540e826e3c702730ad551744cbcc45941588483676eac18e0b9acee5b60104fbae044c4bc6a5431997", ShardID: 1},
	{Index: " 21 ", Address: "one1el7cgl3fmg6qza2zu5rjpvs6cyypsa5szhpzqf", EthAddr: "0xcffd847E29dA34017542e50720b21aC108187690", BLSPublicKey: "d40bb4dab6a18d8792c343a29a9a3c9561ae942d65c1dec222250715389961460431f3ccac400182d58aa7c8649ab787", ShardID: 1},
	{Index: " 22 ", Address: "one1jkgrzvv2qdtw2yz9hrw0hv0ceerhm82qry72dj", EthAddr: "0x959031318a0356e51045b8DcFBb1f8CE477D9d40", BLSPublicKey: "5e8a0871bff7c7c0ed0c0c5564b104b227f821b1248c53a0f4b96779e3d7102c4646fd7c2c3ac8e0915a1dc2666a0b0d", ShardID: 1},
	{Index: " 23 ", Address: "one1mdw2c5vjwfl0utcyjrrnuv5kpamy7uzdfv5w4s", EthAddr: "0xdb5CAc5192727efe2f0490C73e32960F764f704d", BLSPublicKey: "c23286e4ec46065e898e13c8488a9a7d22aefbb0294e78ecd90fc90cbc64248e4a380c002db45a4b0b2aff2979166f03", ShardID: 1},
	{Index: " 24 ", Address: "one1plw8wgmtjchad9c3v386aqtpf9hdfrsgt2x0jh", EthAddr: "0x0fdc77236b962FD69711644fAe8161496eD48e08", BLSPublicKey: "fb7180893b0eb6a5390c91dd6cf474a2db6b7482afa04f258eccb1e092a40389cc721cc3c13b3f30ac4685a15bbd298e", ShardID: 1},
	{Index: " 25 ", Address: "one1xvu8qtqkf6gsh55ap3qr2ezwr3ndnwks3dlwgc", EthAddr: "0x3338702c164E910bD29d0c4035644E1c66d9BaD0", BLSPublicKey: "3da3d98f39efb760ca48c9e792c5ceac60ed256ca0632762e481aa1562fd965bd3d76f1e7c0e0cf652dab3cdd4997981", ShardID: 1},
	{Index: " 26 ", Address: "one14gtzsk0kz6p5ne6uku4yy2q6438352fzhzf2ef", EthAddr: "0xAA162859F6168349E75CB72a42281AaC4F1A2922", BLSPublicKey: "e5d2d5003a0e60cefaa40d3fd8a5338974bf0cd4d2c3e5a7f4468d7f09e443ea406d408352a0df736206ca8eefd55b0e", ShardID: 2},
	{Index: " 27 ", Address: "one1he3ng5w9qgjrzdkenavqsw24wv5e7h5pe0xkk9", EthAddr: "0xbe633451c502243136d99F5808395573299f5E81", BLSPublicKey: "dfd5b66e12f0afe199a85d31219072e1f11ddbea496d92c0709ec12962fdc7b478b3e774f4532a0c895f5e4f73e6be8f", ShardID: 2},
	{Index: " 28 ", Address: "one1hgt3f20tuhjq7asjg8vtd2d9pdr3ef35wxpc6f", EthAddr: "0xbA1714A9EBe5E40F761241D8b6A9a50b471ca634", BLSPublicKey: "4e3d6a1051b1be41a58d9207dc2a3bfe75b8a2092843d498099f5d2c90df66ace4c415280ef83bf1cf7474fb0a68b288", ShardID: 2},
	{Index: " 29 ", Address: "one1j6rw37xdly5lj9un6jumdlcszctu744unhyynu", EthAddr: "0x9686E8F8cDf929f91793D4b9B6ff101617cF56BC", BLSPublicKey: "bbfc3e3bf72c15aef7b6e7abad0bb0b9a82f09c145dde8fefc4776aadcb0f0e74eba99c2899499007ab0e233f2b57d85", ShardID: 2},
	{Index: " 30 ", Address: "one1jdk8lkmuevmz3q8x7snkg5wjku4qcnw928m0ah", EthAddr: "0x936c7FDb7Ccb362880e6F4276451D2b72a0c4dC5", BLSPublicKey: "1df9ea9e1b40151c8e8e45ce20f8578a24df01d0bd946ea7c26d1d7bed9b1a5aa9a63eceacf05d6e42b59419d45e1704", ShardID: 2},
	{Index: " 31 ", Address: "one1js0w47lydksjuycfz8p2yrwlxjntf3dh0yhcae", EthAddr: "0x941EEafbe46da12e130911C2a20Ddf34a6b4C5b7", BLSPublicKey: "a0a5e5549119ab5508ef2910cf46ea8e48994aeb8beb67dfbb1a3d84da9cb0155d370ae7d84ad1608b85ca175ac6488d", ShardID: 2},
	{Index: " 32 ", Address: "one1kyg82d7flnlexqmegnm9x69t89wxjnsc8l03n5", EthAddr: "0xB1107537C9FCfF93037944f65368aB395c694E18", BLSPublicKey: "51565b86e2761ca9f50fbc45bad9dba7545a939b80dd6f6470bb2619d7c62a9bd5c7eeff4063802f68e88195bb52a891", ShardID: 2},
	{Index: " 33 ", Address: "one1l79qj4f60ld5l4rn74rva7xl0r5k8extpyl5s7", EthAddr: "0xFF8a09553a7fdB4fd473f546CEF8DF78e963e4Cb", BLSPublicKey: "8652d7af7e761776e4f4bf759f875f4cc709404a37b3bb5e21a891571a6235dfda5bc82babb4964049572f691f318f80", ShardID: 2},
	{Index: " 34 ", Address: "one1mpvhphe4g47n97q25nmse6a3r0mu6xax3ldm8u", EthAddr: "0xd85970DF35457d32f80aa4F70CEbB11BF7cd1bA6", BLSPublicKey: "d4e40ad31305959fd7fd227400b88dea6a5e40887cf52ca9ef7784b8b3a53f1451953539202e16dd5910afdf77b87a81", ShardID: 2},
	{Index: " 35 ", Address: "one1psjpp6u9hydd5vsc3evx47ch2h9ffzhjfq4pzg", EthAddr: "0x0c2410EB85b91ADa32188E586AfB1755ca948AF2", BLSPublicKey: "fab511cd00b623b83bb3fbb3edf65a939466168cf5e99c61c2291e10b4cceb56434910b58753cc918fc3f55b592d2502", ShardID: 2},
	{Index: " 36 ", Address: "one1xstpjnr5wggzedgr5r58xvxxulrv0qyxfwn3hc", EthAddr: "0x3416194c7472102cB503a0E87330C6e7C6c78086", BLSPublicKey: "ef9581d261157c2e32596f39e90842918dfbda22def4e148c73d438fdf32f6dac6cba72dbc1d8d8eef881b608771ec86", ShardID: 2},
	{Index: " 37 ", Address: "one10tkmez3662chu04cpww74m24gntfau0xxscnu3", EthAddr: "0x7AeDbC8A3Ad2B17e3eb80B9deAED5544D69eF1e6", BLSPublicKey: "9dbd0e1068292794d4f648e4cf90e40c58854a73a22d334bc5a06f4f96dd878380169dcf2d9d176890c6ad5e575a7c02", ShardID: 3},
	{Index: " 38 ", Address: "one12gywpr4z4w4xf68ys4n9t36lhpucwvwfv0qtcf", EthAddr: "0x5208E08EA2Abaa64E8e4856655C75FB8798731C9", BLSPublicKey: "c32631c2e1aab9e6cedb9f986ce57c65c27a850a539effa148a1bd627d98d88958ecbe3c5eb235fbb8f6c4b3767d3488", ShardID: 3},
	{Index: " 39 ", Address: "one15axhtmd3qwcetjhd9svn2rmq006arg87h9juxv", EthAddr: "0xa74D75EdB103b195CAed2C19350F607bf5D1A0fE", BLSPublicKey: "7272d7ae1f1cd007e75b69c54cee89ef908e3386702dba352e9f2a3662a55f229c94e9c8e83d0bc994ceed2d6d7bc30e", ShardID: 3},
	{Index: " 40 ", Address: "one16gqpjs5kn36ep7ave7xh8spepka4eclvfmj4ah", EthAddr: "0xD2001942969C7590fbACCf8D73c0390dbb5ce3eC", BLSPublicKey: "fb8fd94d67c56084035733dbf183d3f65ea834adf3c1dc4e84d23cea251ae690267cc14b6b8b7116a7e33bc1fc3b9704", ShardID: 3},
	{Index: " 41 ", Address: "one17z952d0e026a7p7hwz6aw3se6vjqh2q3c24flc", EthAddr: "0xF08b4535f97ab5dF07d770B5d74619D3240Ba811", BLSPublicKey: "6921264d7710b83a1f5ff30c740b9553a7d047f7c6129d6f1124f0e80d375ad6ba2e64fa7ed61f2192bc64a31672658b", ShardID: 3},
	{Index: " 42 ", Address: "one1am8qejels680q6n69zfergeanmry8jah42jv06", EthAddr: "0xEece0Ccb3F868EF06A7A289391A33D9Ec643CBB7", BLSPublicKey: "158ad720cf1434fe8ac52fd46aed673a779ba4352f5042e7ec562f2805401cd5f07353bdb32057973a5fbfd7a961eb16", ShardID: 3},
	{Index: " 43 ", Address: "one1lz489xqdpjy964d8yargj9s0vscdczua3twx82", EthAddr: "0xF8AA72980d0C885D55A7274689160f6430Dc0B9d", BLSPublicKey: "91ef483463603725e6b819954c5f30ca14e89717f71dcc07d3a75685f215579ee353c097b3d4468023a0f76442a35a86", ShardID: 3},
	{Index: " 44 ", Address: "one1mznftk88zyytdgjqpfgkfux6scu6wddaml6q5e", EthAddr: "0xD8A695D8E71108B6a2400A5164F0da8639a735bd", BLSPublicKey: "337a2bfea65c15be58678a5535ac330910c73ef32d3eb8d61a93702d2afe9812419383617bee6301d673ffc3d8b58183", ShardID: 3},
	{Index: " 45 ", Address: "one1q4rxtjn239g7r7qefe39kwz9quzshq4aj89k6g", EthAddr: "0x054665CA6a8951e1F8194E625B384507050b82Bd", BLSPublicKey: "50ae8d923f33de8d7e0b63e3eea9f4772d7419f5b2a9892eed3854c0adfbf73344c32374b5a77e4cb189824d5c064910", ShardID: 3},
	{Index: " 46 ", Address: "one1r8l6elfzrulyu955wl2ehnnuqu2gl79kqmtvf0", EthAddr: "0x19FFacFd221F3e4e169477d59BCe7C07148FF8B6", BLSPublicKey: "08b9e553f81edd3311a58cb9d5947548b75999e29fc5ee0b45a426ba033a5a5e265cd8bcada141fca23ee125a1409707", ShardID: 3},
	{Index: " 47 ", Address: "one1savspenqaya3m8sa43p8m8pjqqhynpsavnxkcy", EthAddr: "0x875900e660e93b1D9e1DaC427D9C32002e49861D", BLSPublicKey: "32235fed94eaf706c9655d6f440ffcfc2f3781e3ff10a979d790b38889adc740ee4242e699bd915b1d9e7d654fb97c10", ShardID: 3},
	{Index: " 48 ", Address: "one1uqu43kecycvwgmzqrk7v3dr9l5wrtaet3py7jx", EthAddr: "0xe03958DB382618E46c401dBcc8b465fD1c35F72B", BLSPublicKey: "933dc5536f621b03d18157c8ccbca65f833295c851f840ed0d8467b184b28a99a7e43eac467b8398c4b84110403cb80a", ShardID: 3},
	{Index: " 49 ", Address: "one1y8qh5egvl0twg6grnp7dl953rqnhw28qetj7a4", EthAddr: "0x21C17A650CFBD6E46903987CDf969118277728E0", BLSPublicKey: "c8b982ba47eb204c413e7ec451bd125e3550a7df8298e17035326866c54fd4dfbd84684923156f04d9b5cb635444410d", ShardID: 3},
}

func init() {
	// 查看文件 .hmy/expr_deploy_accounts 是否存在
	if _, err := os.Stat(".hmy/expr_deploy_accounts.json"); err == nil {
		// 读取json，反序列化，并设置为ExprHarmonyAccounts
		jsonFile, err := os.Open(".hmy/expr_deploy_accounts.json")
		if err != nil {
			utils.Logger().Error().Msgf("Failed to open expr_deploy_accounts.json: %v", err)
			return
		}
		defer jsonFile.Close()
		json.NewDecoder(jsonFile).Decode(&ExprHarmonyAccounts)
		utils.Logger().Info().Msgf("Loaded %d expr_deploy_accounts from .hmy/expr_deploy_accounts.json", len(ExprHarmonyAccounts))
	}
}
