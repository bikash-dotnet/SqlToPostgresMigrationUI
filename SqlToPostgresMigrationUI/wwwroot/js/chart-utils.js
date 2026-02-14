// wwwroot/js/chart-utils.js
(function () {
    window.charts = window.charts || {};

    function createProgressChart(canvasElement, labels, datasets) {
        if (!canvasElement) return;

        const ctx = canvasElement.getContext('2d');

        // Destroy existing chart if any
        if (window.charts[canvasElement.id]) {
            window.charts[canvasElement.id].destroy();
        }

        window.charts[canvasElement.id] = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true,
                        max: 100,
                        title: {
                            display: true,
                            text: 'Progress (%)'
                        }
                    }
                },
                plugins: {
                    legend: {
                        display: true,
                        position: 'right',
                        labels: {
                            boxWidth: 12,
                            font: {
                                size: 10
                            }
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: function (context) {
                                return `${context.dataset.label}: ${context.raw.toFixed(1)}%`;
                            }
                        }
                    }
                }
            }
        });
    }

    function updateProgressChart(canvasElement, datasets) {
        if (!canvasElement || !window.charts[canvasElement.id]) return;

        const chart = window.charts[canvasElement.id];
        chart.data.datasets = datasets;
        chart.update();
    }

    function saveAsFile(filename, base64Data) {
        const link = document.createElement('a');
        link.download = filename;
        link.href = 'data:text/csv;base64,' + base64Data;
        link.click();
    }

    function printValidationReport() {
        window.print();
    }

    // expose API for Blazor JS interop: call via "chartUtils.createProgressChart" etc.
    window.chartUtils = {
        createProgressChart,
        updateProgressChart,
        saveAsFile,
        printValidationReport
    };
})();