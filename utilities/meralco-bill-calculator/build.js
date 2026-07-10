const { packager } = require('@electron/packager');

async function build() {
  const appPaths = await packager({
    dir: '.',
    name: 'Meralco Bill Calculator',
    platform: 'win32',
    arch: 'x64',
    out: 'dist',
    overwrite: true,
    appVersion: '1.0.0',
    executableName: 'MeralcoBillCalculator',
  });

  console.log('Build complete!');
  console.log('App path:', appPaths[0]);
  console.log('Double-click "MeralcoBillCalculator.exe" to launch.');
}

build().catch(err => {
  console.error('Build failed:', err);
  process.exit(1);
});
