<?php

declare(strict_types=1);

require_once __DIR__ . '/lib/csrf.php';
require_once __DIR__ . '/lib/db.php';

function json_response(array $data, int $status = 200): void
{
    http_response_code($status);
    header('Content-Type: application/json');
    echo json_encode($data);
    exit;
}

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $action = $_POST['action'] ?? '';
    if ($action === 'create_comment') {
        if (!verify_csrf_token($_POST['csrf_token'] ?? null)) {
            json_response(['ok' => false, 'error' => 'Invalid CSRF token'], 400);
        }
        $messageId = (int)($_POST['message_id'] ?? 0);
        $nickname = (string)($_POST['nickname'] ?? '');
        $body = (string)($_POST['body'] ?? '');
        if ($messageId <= 0) {
            json_response(['ok' => false, 'error' => 'Message ID is required'], 400);
        }
        try {
            $comment = Database::createComment($messageId, $nickname, $body);
            json_response(['ok' => true, 'comment' => $comment]);
        } catch (Throwable $e) {
            json_response(['ok' => false, 'error' => $e->getMessage()], 400);
        }
    }
    json_response(['ok' => false, 'error' => 'Unknown action'], 400);
}

$messages = Database::fetchMessages();
$messageIds = array_column($messages, 'id');
$comments = Database::fetchCommentsForMessages($messageIds);

include __DIR__ . '/views/home.php';
