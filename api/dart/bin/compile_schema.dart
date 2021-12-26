import 'dart:io';

void main(List<String> args) async {
  String tlfsc = 'build/tlfs/tlfsc';
  if (Platform.isWindows) {
    tlfsc = 'build/tlfs/tlfsc.exe';
  }
  String appname = args[1];
  final result = await Process.run(tlfsc, [
    '-i',
    'assets/' + appname + '.tlfs',
    '-o',
    'assets/' + appname + '.tlfs.rkyv'
  ]);
  stdout.write(result.stdout);
  stderr.write(result.stderr);
}
