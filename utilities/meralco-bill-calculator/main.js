const { app, BrowserWindow } = require('electron');

function createWindow() {
  const win = new BrowserWindow({
    width: 520,
    height: 720,
    resizable: true,
    minWidth: 420,
    minHeight: 600,
    center: true,
    title: 'Meralco Bill Calculator',
    backgroundColor: '#0f172a',
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
    },
  });

  win.setMenu(null);
  win.loadFile('index.html');
}

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  app.quit();
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    createWindow();
  }
});
