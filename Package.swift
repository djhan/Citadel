// swift-tools-version:5.9
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "Citadel",
    platforms: [
        .macOS(.v14),
        .iOS(.v17)
    ],
    products: [
        .library(
            name: "Citadel",
            targets: ["Citadel"]
        ),
    ],
    dependencies: [
        // .package(path: "/Users/joannisorlandos/git/joannis/swift-nio-ssh"),
        // EdgeView 패치: ChildChannelStateMachine.sendChannelWindowAdjust가 채널이 막 닫힌
        // 직후 도착하는 인바운드 읽기(window adjust 자동 생성)와의 레이스로 preconditionFailure
        // 크래쉬를 내던 문제를 고친 포크. Wellz26/swift-nio-ssh의 citadel2 브랜치(0.3.6, 커밋
        // a05e6bb) 위에 패치 한 개만 얹었다.
        .package(url: "https://github.com/djhan/swift-nio-ssh.git", revision: "a4b54fd32ee612bacf19a956c2046ec65bb3a542"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.81.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
        .package(url: "https://github.com/attaswift/BigInt.git", from: "5.2.0"),
        .package(url: "https://github.com/apple/swift-crypto.git", from: "3.12.3"),
        .package(url: "https://github.com/mtynior/ColorizeSwift.git", from: "1.5.0"),
    ],
    targets: [
        .target(
            name: "CCitadelBcrypt",
            cSettings: [
                .unsafeFlags(["-w"])
            ]
        ),
        .target(
            name: "Citadel",
            dependencies: [
                .target(name: "CCitadelBcrypt"),
                .product(name: "NIOSSH", package: "swift-nio-ssh"),
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "Crypto", package: "swift-crypto"),
                .product(name: "_CryptoExtras", package: "swift-crypto"),
                .product(name: "BigInt", package: "BigInt"),
                .product(name: "Logging", package: "swift-log"),
            ],
            swiftSettings: [
                .unsafeFlags(["-suppress-warnings"])
            ]
        ),
        .executableTarget(
            name: "CitadelServerExample",
            dependencies: [
                "Citadel",
                .product(name: "ColorizeSwift", package: "ColorizeSwift")
            ]),
        .testTarget(
            name: "CitadelTests",
            dependencies: [
                "Citadel",
                .product(name: "NIOSSH", package: "swift-nio-ssh"),
                .product(name: "BigInt", package: "BigInt"),
                .product(name: "Logging", package: "swift-log"),
            ]
        ),
    ]
)
