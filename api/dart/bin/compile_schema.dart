import 'dart:io';

void main() async {
  final result = await Process.run('build/tlfs/tlfsc', ['-i', 'assets/todoapp.tlfs', '-o', 'assets/todoapp.tlfs.rkyv']);
  stdout.write(result.stdout);
  stderr.write(result.stderr);
}
