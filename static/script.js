// 电影详情页面的星级评分交互
document.addEventListener('DOMContentLoaded', function() {
    const ratingInputs = document.querySelectorAll('input[name="rating"]');
    
    ratingInputs.forEach(input => {
        input.addEventListener('change', function() {
            // 可以添加一些视觉反馈
            ratingInputs.forEach(r => {
                const label = r.nextElementSibling;
                if (parseInt(r.value) <= parseInt(this.value)) {
                    label.classList.add('text-warning');
                } else {
                    label.classList.remove('text-warning');
                }
            });
        });
    });
}); 