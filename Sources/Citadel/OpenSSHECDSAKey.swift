import Crypto
import Foundation
import NIO

// OpenSSH 포맷 ECDSA 개인 키(`ecdsa-sha2-nistp256/384/521`) 지원.
// - `OpenSSH.PrivateKey<SSHKey>.init(string:decryptionKey:)`가 bcrypt_pbkdf·aes-ctr 복호화와 check·패딩 검증을 그대로 처리하고,
//   여기서는 곡선별 공개 키·개인 키 블록의 읽기만 정의한다.
// - 블록 구조(https://github.com/openssh/openssh-portable/blob/master/PROTOCOL.key):
//   - 공개 키: `string keytype`(파서가 소비) + `string curve` + `string Q`(비압축 점)
//   - 개인 키: `string keytype`(파서가 소비) + `string curve` + `string Q` + `mpint d` (+ `string comment`, 파서가 소비)

/// OpenSSH 포맷 ECDSA 공개 키의 곡선별 차이(곡선 이름)만 정의한다. 읽기는 프로토콜 확장이 공유한다.
protocol OpenSSHECDSAPublicKey: ByteBufferConvertible {
    /// OpenSSH 곡선 이름(`nistp256` 등)
    static var curveName: String { get }
    init<Bytes: ContiguousBytes>(x963Representation: Bytes) throws
    var x963Representation: Data { get }
}

extension OpenSSHECDSAPublicKey {
    static func read(consuming buffer: inout ByteBuffer) throws -> Self {
        guard let curveName = buffer.readSSHString(), curveName == Self.curveName else {
            throw InvalidOpenSSHKey.invalidPublicKeyPrefix
        }
        guard var pointBuffer = buffer.readSSHBuffer(),
              let point = pointBuffer.readBytes(length: pointBuffer.readableBytes) else {
            throw InvalidOpenSSHKey.missingPublicKeyBuffer
        }
        return try Self(x963Representation: point)
    }

    /// 키 내보내기는 지원하지 않는다(파서에 필요 없음, RSA 구현과 같음).
    func write(to buffer: inout ByteBuffer) -> Int {
        0
    }
}

/// OpenSSH 포맷 ECDSA 개인 키의 곡선별 차이(곡선 이름·스칼라 길이)만 정의한다. 읽기와 `OpenSSHPrivateKey` 요구사항은 프로토콜 확장이 공유한다.
protocol OpenSSHECDSAPrivateKey: OpenSSHPrivateKey where PublicKey: OpenSSHECDSAPublicKey {
    /// 개인 스칼라(`d`)의 바이트 길이 — P-256 32, P-384 48, P-521 66.
    static var scalarLength: Int { get }
    init<Bytes: ContiguousBytes>(rawRepresentation: Bytes) throws
    var publicKey: PublicKey { get }
}

extension OpenSSHECDSAPrivateKey {
    static var publicKeyPrefix: String { "ecdsa-sha2-" + PublicKey.curveName }
    static var privateKeyPrefix: String { publicKeyPrefix }
    static var keyType: OpenSSH.KeyType { OpenSSH.KeyType(rawValue: publicKeyPrefix)! }

    static func read(consuming buffer: inout ByteBuffer) throws -> Self {
        guard let curveName = buffer.readSSHString(), curveName == PublicKey.curveName else {
            throw InvalidOpenSSHKey.invalidPublicKeyInPrivateKey
        }
        guard var pointBuffer = buffer.readSSHBuffer(),
              let point = pointBuffer.readBytes(length: pointBuffer.readableBytes),
              var scalarBuffer = buffer.readSSHBuffer(),
              let mpint = scalarBuffer.readBytes(length: scalarBuffer.readableBytes) else {
            throw InvalidOpenSSHKey.missingPrivateKeyBuffer
        }

        // `mpint`는 부호용 선행 0x00이 붙거나(최상위 비트가 1인 경우) 앞자리 0이 빠질 수 있다 — 선행 0을 걷어낸 뒤 스칼라 길이에 맞춰 왼쪽을 0으로 채운다.
        let significantBytes = mpint.drop(while: { $0 == 0 })
        guard significantBytes.count <= scalarLength else {
            throw InvalidOpenSSHKey.invalidLayout
        }
        let scalar = [UInt8](repeating: 0, count: scalarLength - significantBytes.count) + significantBytes

        let privateKey = try Self(rawRepresentation: scalar)
        // Ed25519와 같이, 복원한 키의 공개 키가 키 파일에 적힌 공개 키와 같은지 검증한다.
        guard privateKey.publicKey.x963Representation == Data(point) else {
            throw InvalidOpenSSHKey.invalidPublicKeyInPrivateKey
        }
        return privateKey
    }

    /// 키 내보내기는 지원하지 않는다(파서에 필요 없음, RSA 구현과 같음).
    func write(to buffer: inout ByteBuffer) -> Int {
        0
    }
}

extension P256.Signing.PublicKey: OpenSSHECDSAPublicKey {
    static var curveName: String { "nistp256" }
}

extension P384.Signing.PublicKey: OpenSSHECDSAPublicKey {
    static var curveName: String { "nistp384" }
}

extension P521.Signing.PublicKey: OpenSSHECDSAPublicKey {
    static var curveName: String { "nistp521" }
}

extension P256.Signing.PrivateKey: OpenSSHECDSAPrivateKey {
    typealias PublicKey = P256.Signing.PublicKey
    static var scalarLength: Int { 32 }

    /// OpenSSH 포맷 ECDSA P-256 개인 키 문자열에서 키를 만든다(`init(sshEd25519:decryptionKey:)`와 같은 모양).
    /// - Parameters:
    ///   - key: `-----BEGIN OPENSSH PRIVATE KEY-----`로 시작하는 키 문자열.
    ///   - decryptionKey: 암호화된 키의 passphrase. 없으면 `nil`.
    public init(sshECDSA key: String, decryptionKey: Data? = nil) throws {
        self = try OpenSSH.PrivateKey<Self>(string: key, decryptionKey: decryptionKey).privateKey
    }
}

extension P384.Signing.PrivateKey: OpenSSHECDSAPrivateKey {
    typealias PublicKey = P384.Signing.PublicKey
    static var scalarLength: Int { 48 }

    /// OpenSSH 포맷 ECDSA P-384 개인 키 문자열에서 키를 만든다(`init(sshEd25519:decryptionKey:)`와 같은 모양).
    /// - Parameters:
    ///   - key: `-----BEGIN OPENSSH PRIVATE KEY-----`로 시작하는 키 문자열.
    ///   - decryptionKey: 암호화된 키의 passphrase. 없으면 `nil`.
    public init(sshECDSA key: String, decryptionKey: Data? = nil) throws {
        self = try OpenSSH.PrivateKey<Self>(string: key, decryptionKey: decryptionKey).privateKey
    }
}

extension P521.Signing.PrivateKey: OpenSSHECDSAPrivateKey {
    typealias PublicKey = P521.Signing.PublicKey
    static var scalarLength: Int { 66 }

    /// OpenSSH 포맷 ECDSA P-521 개인 키 문자열에서 키를 만든다(`init(sshEd25519:decryptionKey:)`와 같은 모양).
    /// - Parameters:
    ///   - key: `-----BEGIN OPENSSH PRIVATE KEY-----`로 시작하는 키 문자열.
    ///   - decryptionKey: 암호화된 키의 passphrase. 없으면 `nil`.
    public init(sshECDSA key: String, decryptionKey: Data? = nil) throws {
        self = try OpenSSH.PrivateKey<Self>(string: key, decryptionKey: decryptionKey).privateKey
    }
}
