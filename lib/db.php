<?php

declare(strict_types=1);

/**
 * Database helpers for Cave of Conspiracies.
 */
class Database
{
    private static ?\PDO $pdo = null;

    public static function pdo(): \PDO
    {
        if (self::$pdo === null) {
            $dsn = getenv('DB_DSN') ?: 'mysql:host=localhost;dbname=cave_of_conspiracies;charset=utf8mb4';
            $user = getenv('DB_USER') ?: 'root';
            $password = getenv('DB_PASSWORD') ?: '';
            $options = [
                \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
                \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
                \PDO::ATTR_EMULATE_PREPARES => false,
            ];
            self::$pdo = new \PDO($dsn, $user, $password, $options);
        }
        return self::$pdo;
    }

    public static function fetchMessages(): array
    {
        $stmt = self::pdo()->prepare('SELECT id, nickname, body, created_at FROM messages ORDER BY created_at DESC');
        $stmt->execute();
        return $stmt->fetchAll();
    }

    public static function fetchCommentsForMessages(array $messageIds): array
    {
        if (empty($messageIds)) {
            return [];
        }
        $placeholders = implode(',', array_fill(0, count($messageIds), '?'));
        $stmt = self::pdo()->prepare("SELECT id, message_id, nickname, body, created_at FROM comments WHERE message_id IN ($placeholders) ORDER BY created_at ASC");
        $stmt->execute($messageIds);
        $rows = $stmt->fetchAll();
        $grouped = [];
        foreach ($rows as $row) {
            $grouped[(int)$row['message_id']][] = $row;
        }
        return $grouped;
    }

    public static function createComment(int $messageId, string $nickname, string $body): array
    {
        $body = trim($body);
        if ($body === '') {
            throw new \InvalidArgumentException('Comment body is required.');
        }
        if (mb_strlen($body) > 240) {
            throw new \InvalidArgumentException('Comment body exceeds 240 characters.');
        }
        $nickname = trim($nickname) ?: 'Anonymous';
        if (mb_strlen($nickname) > 100) {
            $nickname = mb_substr($nickname, 0, 100);
        }
        $stmt = self::pdo()->prepare('INSERT INTO comments (message_id, nickname, body, created_at) VALUES (:message_id, :nickname, :body, NOW())');
        $stmt->execute([
            ':message_id' => $messageId,
            ':nickname' => $nickname,
            ':body' => $body,
        ]);
        $id = (int)self::pdo()->lastInsertId();
        $createdAtStmt = self::pdo()->prepare('SELECT created_at FROM comments WHERE id = :id');
        $createdAtStmt->execute([':id' => $id]);
        $createdAt = $createdAtStmt->fetchColumn();

        return [
            'id' => $id,
            'message_id' => $messageId,
            'nickname' => $nickname,
            'body' => $body,
            'created_at' => $createdAt,
        ];
    }
}
