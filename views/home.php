<?php
function h(string $value): string
{
    return htmlspecialchars($value, ENT_QUOTES, 'UTF-8');
}
?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Cave of Conspiracies</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 2rem; background-color: #0b0b0f; color: #e8e8f0; }
        h1 { margin-bottom: 1rem; }
        .message { border: 1px solid #2c2c35; border-radius: 8px; padding: 1rem; margin-bottom: 1.5rem; background: #14141a; }
        .message-meta { font-size: 0.9rem; color: #a5a5b2; margin-bottom: 0.35rem; }
        .message-body { margin: 0.5rem 0 1rem; line-height: 1.5; }
        .comments { margin-top: 0.5rem; padding-left: 0.5rem; border-left: 2px solid #2f2f3b; }
        .comment { margin-bottom: 0.75rem; padding: 0.5rem; border-radius: 6px; background: #1c1c26; }
        .comment-meta { font-size: 0.85rem; color: #9ba0b6; display: flex; justify-content: space-between; }
        .comment-body { margin-top: 0.35rem; white-space: pre-wrap; }
        .comment-form { display: grid; gap: 0.5rem; margin-top: 0.75rem; }
        .comment-form input[type="text"], .comment-form textarea { width: 100%; padding: 0.5rem; border-radius: 4px; border: 1px solid #3c3c48; background: #0f0f16; color: #e8e8f0; }
        .comment-form button { width: fit-content; padding: 0.45rem 0.9rem; border: none; border-radius: 4px; background: #4c68ff; color: white; cursor: pointer; }
        .comment-form button:disabled { opacity: 0.6; cursor: not-allowed; }
        .error { color: #ff6b81; font-size: 0.9rem; min-height: 1.2rem; }
        .empty-state { color: #9ba0b6; font-style: italic; }
    </style>
</head>
<body>
    <h1>Cave of Conspiracies</h1>

    <?php if (empty($messages)): ?>
        <p class="empty-state">No messages yet.</p>
    <?php endif; ?>

    <?php foreach ($messages as $message): ?>
        <article class="message" data-message-id="<?= h((string)$message['id']); ?>">
            <div class="message-meta">
                <strong><?= h($message['nickname'] ?? 'Anonymous'); ?></strong>
                <span>• <?= h($message['created_at'] ?? ''); ?></span>
            </div>
            <div class="message-body"><?= nl2br(h($message['body'] ?? '')); ?></div>

            <section class="comments" id="comments-<?= h((string)$message['id']); ?>">
                <?php foreach ($comments[$message['id']] ?? [] as $comment): ?>
                    <div class="comment" data-comment-id="<?= h((string)$comment['id']); ?>">
                        <div class="comment-meta">
                            <strong><?= h($comment['nickname']); ?></strong>
                            <span><?= h($comment['created_at']); ?></span>
                        </div>
                        <div class="comment-body"><?= nl2br(h($comment['body'])); ?></div>
                    </div>
                <?php endforeach; ?>
            </section>

            <form class="comment-form" data-message-id="<?= h((string)$message['id']); ?>" method="post">
                <input type="hidden" name="action" value="create_comment">
                <input type="hidden" name="message_id" value="<?= h((string)$message['id']); ?>">
                <input type="hidden" name="csrf_token" value="<?= h(csrf_token()); ?>">
                <input type="text" name="nickname" placeholder="Nickname (optional)">
                <textarea name="body" rows="3" maxlength="240" placeholder="Add a reply (max 240 chars)" required></textarea>
                <div class="error" aria-live="polite"></div>
                <button type="submit">Post comment</button>
            </form>
        </article>
    <?php endforeach; ?>

    <script>
        function escapeHtml(str) {
            const div = document.createElement('div');
            div.innerText = str;
            return div.innerHTML;
        }

        function renderComment(comment) {
            const wrapper = document.createElement('div');
            wrapper.className = 'comment';
            wrapper.dataset.commentId = comment.id;
            wrapper.innerHTML = `
                <div class="comment-meta">
                    <strong>${escapeHtml(comment.nickname)}</strong>
                    <span>${escapeHtml(comment.created_at)}</span>
                </div>
                <div class="comment-body">${escapeHtml(comment.body).replace(/\n/g, '<br>')}</div>
            `;
            return wrapper;
        }

        document.querySelectorAll('.comment-form').forEach(form => {
            form.addEventListener('submit', async (event) => {
                event.preventDefault();
                const errorEl = form.querySelector('.error');
                errorEl.textContent = '';
                const submitBtn = form.querySelector('button[type="submit"]');
                submitBtn.disabled = true;

                try {
                    const response = await fetch(form.action || window.location.href, {
                        method: 'POST',
                        body: new FormData(form)
                    });
                    const data = await response.json();
                    if (!data.ok) {
                        throw new Error(data.error || 'Unable to save comment');
                    }
                    const commentNode = renderComment(data.comment);
                    const container = document.getElementById(`comments-${form.dataset.messageId}`);
                    container.appendChild(commentNode);
                    form.reset();
                } catch (err) {
                    errorEl.textContent = err.message;
                } finally {
                    submitBtn.disabled = false;
                }
            });
        });
    </script>
</body>
</html>
