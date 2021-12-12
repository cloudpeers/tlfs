import 'dart:io';

enum Artefact {
  libtlfs,
  tlfsc,
}

enum Target {
  linux,
  macos,
  android,
  ios,
}

void download(Artefact artefact, Target target, String out) async {
  await Directory(out).create(recursive: true);
  final artefacts = [
    'libtlfs',
    'tlfsc',
  ];
  final targets = [
    'x86_64-unknown-linux-gnu',
    'x86_64-apple-darwin',
    'aarch64-linux-android',
    'aarch64-apple-ios',
  ];
  final asset = artefacts[artefact.index] + '.' + targets[target.index] + '.tar.zst';
  final result = await Process.run(
    'gh', ['release', 'download', '-p', asset, '-R', 'cloudpeers/tlfs'],
    workingDirectory: out);
  stdout.write(result.stdout);
  stderr.write(result.stderr);
  var tar = 'tar';
  if (Platform.isMacOS) {
    tar = 'gtar';
  }
  final result2 = await Process.run(
    tar, ['--zstd', '-xf', asset],
    workingDirectory: out);
  stdout.write(result2.stdout);
  stderr.write(result2.stderr);
}

void main() {
  if (Platform.isLinux) {
    download(Artefact.tlfsc, Target.linux, 'build/tlfs');
    download(Artefact.libtlfs, Target.linux, 'build/tlfs/linux');
    download(Artefact.libtlfs, Target.android, 'build/tlfs/android');
  }
  if (Platform.isMacOS) {
    download(Artefact.tlfsc, Target.macos, 'build/tlfs');
    download(Artefact.libtlfs, Target.macos, 'build/tlfs/macos');
    download(Artefact.libtlfs, Target.android, 'build/tlfs/android');
    download(Artefact.libtlfs, Target.ios, 'build/tlfs/ios');
  }
}
